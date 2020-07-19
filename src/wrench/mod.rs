use {
    anyhow::anyhow,
    anyhow::{Context, Error},
    //async_std::prelude::*,
    console::style,
    encoding_rs::WINDOWS_1252,
    encoding_rs_io::DecodeReaderBytesBuilder,
    full_moon_compat_luaparse::Chunk,
    indicatif::{ProgressBar, ProgressStyle},
    memmap::Mmap,
    miniserde::{json, Serialize},
    quick_xml::{events::Event as XmlEvent, Reader as XmlReader},
    std::{
        collections::BTreeMap,
        fs::{read_dir, File},
        io::{BufReader, Read},
        path::{Path, PathBuf},
        time::Instant,
    },
    structopt::StructOpt,
};

pub mod ser;
use ser::*;
pub mod de;
use de::*;
pub mod data;
use data::Filter;

mod locale;
use assembly::fdb::align::{Database, Table};
use crossbeam_channel::Sender;
use fdb::{
    dblpaged_suffix, paged_suffix, setup_group_by, unpaged_suffix, CollectRow, StoreMulti,
    StoreSimple,
};
use io::Write;
use locale::{expect_attribute, expect_end, expect_start, expect_text};
use log::trace;
use std::{borrow::Cow, io, thread, time::Duration};
use thread::JoinHandle;

mod fdb;

#[derive(StructOpt)]
pub struct WrenchOpt {
    /// The base `res` folder of a LU installation
    prefix: PathBuf,
    /// The directory to place `lu-json` into.
    #[allow(unused)]
    output: PathBuf,
    /// The path to the configuration json
    config: PathBuf,

    /// Whether to serialize scripts
    #[structopt(short = "S", long = "scripts")]
    scripts: bool,
    /// Whether to serialize locale
    #[structopt(short = "L", long = "locale")]
    locale: bool,
    /// Whether to serialize tables
    #[structopt(short = "T", long = "tables")]
    tables: bool,
}

pub struct WrenchState {
    pub opt: WrenchOpt,
    pub sender: Sender<Option<(PathBuf, String)>>,
    pub join_handle: JoinHandle<(usize, Duration)>,
}

type Res<T> = Result<T, Error>;
type EmptyResult = Res<()>;

impl WrenchState {
    fn load_config(&self) -> Res<LoadConfig> {
        let mut buf = String::new();
        let f_config = File::open(&self.opt.config)?;
        let mut b_config = BufReader::new(f_config);
        b_config.read_to_string(&mut buf)?;
        let config = json::from_str(&buf)?;
        Ok(config)
    }

    fn make_file<D: Serialize>(&self, file: &Path, data: &D) -> EmptyResult {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let out = match catch_unwind(AssertUnwindSafe(|| json::to_string(data))) {
            Ok(out) => out,
            _e => {
                return Err(anyhow!("Failed to write `{}`", file.display()));
            }
        };

        let p_out = self.opt.output.join(file);

        std::fs::create_dir_all(p_out.parent().unwrap())?;
        self.sender.send(Some((p_out, out)))?;
        Ok(())
    }

    fn run_store_table(
        &self,
        table: Table,
        config: &LoadConfig,
        path_tables: &Path,
        progress_bar: ProgressBar,
    ) -> EmptyResult {
        let name = table.name();

        let t = match config.tables.get(&*name) {
            Some(t) => t,
            None => {
                trace!("Ignoring table {}", name);
                return Ok(());
            }
        };

        progress_bar.set_message(&name);
        let columns = t.make_column_vecs(table)?;

        // Set primary key
        let pk_index = {
            if let Some(key) = t.pk.as_ref() {
                columns
                    .iter()
                    .position(|k| k.name.as_ref() == key.as_str())
                    .ok_or_else(|| anyhow!("Invalid PK name: `{}`!", key))?
            } else {
                0
            }
        };

        // Setup indexer
        let indexer = match t.index.as_ref() {
            Some(i) => Cow::Borrowed(i),
            None => Cow::Owned({
                // default indexer
                let mut map = BTreeMap::new();
                map.insert("id".to_string(), pk_index);
                map
            }),
        };

        // Setup group by
        let mut group_by = setup_group_by(&t.group_by, &columns);

        // Setup mapper
        let default_mapper = BTreeMap::new();
        let mapper = t.map.as_ref().unwrap_or(&default_mapper);

        // Setup filters/ignores
        let ignore: Vec<Filter> = t.setup_filters(&columns[..])?;

        let collect_row = CollectRow::new(&columns[..], mapper);
        let path_table = {
            let mut path = path_tables.to_owned();
            for part in name.split('_') {
                path.push(part);
            }
            path
        };
        let path_table_index = path_table.join("index.json");

        match t.level {
            0 => {
                // Store to a single json file
                let j_single_index = {
                    let mut links = BTreeMap::new();
                    let href = String::from("/") + &path_table_index.to_string_lossy();
                    links.insert("self", Rel { href });
                    let mut data = BTreeMap::new();
                    data.insert(&name, {
                        let mut arr = Vec::new();
                        for raw_row in table.row_iter() {
                            let obj = collect_row.invoke(raw_row, None)?;
                            arr.push(obj);
                        }
                        arr
                    });
                    HAL {
                        _links: links,
                        _embedded: data,
                    }
                };
                self.make_file(&path_table_index, &j_single_index)?;
            }
            lvl if lvl < 4/* fixme -*/ => {
                let paginator: fn(i32, &mut PathBuf) = match lvl {
                    1 => unpaged_suffix,
                    2 => paged_suffix,
                    3 => dblpaged_suffix,
                    _ => todo!(),
                };

                // Store to a single json file
                let mut links = BTreeMap::new();
                let href = String::from("/") + &path_table_index.to_string_lossy();
                links.insert("self", Rel { href });
                let mut data = BTreeMap::new();

                if let Some(key) = &t.many {
                    let multi = StoreMulti::new(
                        pk_index, key,
                        &path_table,
                        paginator, indexer.as_ref(),
                        &collect_row,
                        &columns[..],
                        &mut group_by[..],
                        |a,b| self.make_file(a, b)
                    );
                    let value = multi.invoke(table)?;
                    data.insert(&name, value);
                } else {
                    let simple = StoreSimple::new(
                        &ignore[..], pk_index,
                        &path_table,
                        paginator,
                        indexer.as_ref(),
                        collect_row,
                        &mut group_by[..],
                        |a,b| self.make_file(a, b),
                    );
                    let value = simple.invoke(table)?;
                    data.insert(&name, value);
                }

                let j_index = HAL {
                    _links: links,
                    _embedded: data,
                };
                self.make_file(&path_table_index, &j_index)?;

                let path_table_group_by = path_table.join("groupBy");
                for group in group_by {
                    let mut path_table_group = path_table_group_by.join(&group.group_name);
                    path_table_group.set_extension("json");
                    self.make_file(&path_table_group, &group.result())?;
                }
            }
            _ => {
                progress_bar.println(format!("{:?}", t));
            }
        }

        Ok(())
    }

    pub fn process_locale_xml(&self, config: &LoadConfig, path: &Path) -> Res<()> {
        let mut table_iter = config.tables.keys();
        let mut table_current = None;
        let mut table_next = table_iter.next();

        let file = File::open(path).unwrap();
        let file = BufReader::new(file);

        let mut reader = XmlReader::from_reader(file);
        reader.trim_text(true);

        let mut buf = Vec::new();

        // The `Reader` does not implement `Iterator` because it outputs borrowed data (`Cow`s)
        if let Ok(XmlEvent::Decl(_)) = reader.read_event(&mut buf) {}
        buf.clear();

        let _ = expect_start("localization", &mut reader, &mut buf)?;
        //println!("<localization>");
        buf.clear();

        let e_locales = expect_start("locales", &mut reader, &mut buf)?;
        //println!("<locales>");
        let locale_count = expect_attribute("count", &reader, &e_locales)?;
        buf.clear();

        for _ in 0..locale_count {
            let _ = expect_start("locale", &mut reader, &mut buf)?;
            //print!("<locale>");
            buf.clear();

            let _locale = expect_text(&mut reader, &mut buf)?;
            //print!("{}", locale);
            buf.clear();

            let _ = expect_end("locale", &mut reader, &mut buf)?;
            //println!("</locale>");
            buf.clear();
        }

        let _ = expect_end("locales", &mut reader, &mut buf)?;
        buf.clear();
        //println!("</locales>");

        let mut tables = BTreeMap::new();

        let e_locales = expect_start("phrases", &mut reader, &mut buf)?;
        //println!("<phrases>");
        let phrase_count = expect_attribute("count", &reader, &e_locales)?;
        buf.clear();

        let started = Instant::now();
        let progress_bar = ProgressBar::new(phrase_count as u64);
        progress_bar.set_prefix("Loading");
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:>12.bold.cyan} [{bar:55}] {pos}/{len}: {msg}")
                .progress_chars("=> "),
        );

        for _ in 0..phrase_count {
            let e_phrase = expect_start("phrase", &mut reader, &mut buf)?;
            let id: String = expect_attribute("id", &reader, &e_phrase)?;

            if let Some(mut ntbl) = table_next {
                while id > *ntbl {
                    table_next = table_iter.next();
                    if id.starts_with(ntbl) {
                        table_current = Some(ntbl);
                        tables.insert(ntbl, BTreeMap::new());
                        //println!("TABLE: {}", ntbl);
                        progress_bar.set_message(ntbl);
                        break;
                    } else if let Some(nntbl) = table_next {
                        ntbl = nntbl;
                    } else {
                        table_current = None;
                        break;
                    }
                }
            }

            let mut add_to_table = None;
            if let Some(tbl) = table_current {
                if id.starts_with(tbl) {
                    let rest = unsafe { id.get_unchecked(tbl.len()..) };
                    let mut iter = rest.chars();
                    assert_eq!(iter.next(), Some('_'));
                    let mut part_iter = iter.as_str().splitn(2, '_');
                    let id_str = part_iter.next().unwrap();
                    let id: usize = id_str.parse()?;
                    let field = part_iter.next().unwrap();
                    add_to_table = Some((tbl, id, field.to_string()));
                } else {
                    table_current = None;
                }
            }

            //println!("ID: {}", id);
            buf.clear();

            let mut translation = None;

            loop {
                let event = reader.read_event(&mut buf)?;
                let e_translation = match event {
                    XmlEvent::End(e) => {
                        if e.name() == b"phrase" {
                            break;
                        } else {
                            let name_str = reader.decode(e.name())?;
                            return Err(anyhow!("Unexpected end tag </{}>", name_str));
                        }
                    }
                    XmlEvent::Start(e) => {
                        if e.name() == b"translation" {
                            e
                        } else {
                            let name_str = reader.decode(e.name())?;
                            return Err(anyhow!("Unexpected tag <{}>", name_str));
                        }
                    }
                    _ => panic!(),
                };
                let locale: String = expect_attribute("locale", &reader, &e_translation)?;
                buf.clear();

                let trans = expect_text(&mut reader, &mut buf)?;
                if &locale == "en_US" {
                    translation = Some(trans);
                }
                buf.clear();

                let _ = expect_end("translation", &mut reader, &mut buf)?;
                buf.clear();
            }

            if let (Some((table, id, field)), Some(translation)) = (add_to_table, translation) {
                let table = tables.get_mut(table).unwrap();
                let page_index = id / 256;
                let page = table.entry(page_index).or_insert_with(BTreeMap::new);
                let row = page.entry(id).or_insert_with(BTreeMap::new);
                row.insert(field, translation);
            }

            progress_bar.inc(1);
        }

        progress_bar.finish_and_clear();
        let elapsed = started.elapsed();
        println!(
            "{:>12} locale in {}.{}s",
            style("Finished").green().bold(),
            elapsed.as_secs(),
            elapsed.subsec_millis(),
        );

        let _ = expect_end("phrases", &mut reader, &mut buf)?;
        //println!("</phrases>");
        buf.clear();

        let path = Path::new("lu-json");
        let path_locale = path.join("locale");

        for (table_name, table) in tables {
            let path_table = path_locale.join(table_name);
            for (page_index, page) in table {
                let path_page = path_table.join(&format!("{}.json", page_index));
                self.make_file(&path_page, &page)?;
            }
        }

        Ok(())
    }

    fn run_cdc(&self, config: &LoadConfig, path: &Path) -> EmptyResult {
        let started = Instant::now();

        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let buffer: &[u8] = &mmap;
        let db = Database::new(buffer);

        let tables = db.tables();
        //let mut loader = BufReader::new(file);

        //let h = loader.get_header()?;
        //let thl = loader.get_table_header_list(h)?;

        //let thlv: Vec<_> = thl.into();

        let progress_bar = ProgressBar::new(tables.len() as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:>12.bold.cyan} [{bar:55}] {pos}/{len}: {msg}")
                .progress_chars("=> "),
        );
        progress_bar.set_prefix("Loading");
        progress_bar.reset();

        let iter = tables.iter();

        let path = Path::new("lu-json");
        let path_tables = path.join("tables");

        for th in iter {
            self.run_store_table(th, &config, &path_tables, progress_bar.clone())?;
            progress_bar.inc(1);
        }

        progress_bar.finish_and_clear();
        let elapsed = started.elapsed();
        println!(
            "{:>12} tables in {}.{}s",
            style("Finished").green().bold(),
            elapsed.as_secs(),
            elapsed.subsec_millis(),
        );

        Ok(())
    }

    pub fn process_scripts_folder(
        &self,
        config: &LoadConfig,
        path: &Path,
        root: &Path,
        path_out_scripts: &Path,
        progress_bar: ProgressBar,
    ) -> Res<()> {
        for entry in read_dir(path).expect("iter files in input") {
            if let Ok(entry) = entry {
                let path = entry.path();
                let relative = path.strip_prefix(root)?;
                progress_bar.set_message(&format!("{}", relative.display()));

                if path.is_dir() {
                    self.process_scripts_folder(
                        config,
                        &path,
                        root,
                        path_out_scripts,
                        progress_bar.clone(),
                    )?;
                } else if path.is_file() && path.extension() == Some(std::ffi::OsStr::new("lua")) {
                    let lua_file = std::fs::File::open(&path)?;
                    let lua_buf_reader = BufReader::new(lua_file);
                    let mut reader = DecodeReaderBytesBuilder::new()
                        .encoding(Some(WINDOWS_1252))
                        .build(lua_buf_reader);
                    let mut lua_text = String::new();
                    reader.read_to_string(&mut lua_text)?;

                    let res = full_moon::parse(&lua_text);
                    match res {
                        Ok(lua_ast) => {
                            let chunk = Chunk::wrap(&lua_ast);
                            let mut path_out_chunk = path_out_scripts.join(relative);
                            path_out_chunk.set_extension("lua.json");
                            self.make_file(&path_out_chunk, &chunk)?;
                        }
                        Err(error) => {
                            progress_bar.println(&format!("File: {}", path.display()));
                            progress_bar.println(&format!("{}", error));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn process_scripts(&self, config: &LoadConfig, path: &Path) -> Res<()> {
        let path_out = Path::new("lu-json");
        let path_out_scripts = path_out.join("scripts");

        let started = Instant::now();
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.set_prefix("Loading");
        progress_bar.enable_steady_tick(50);
        progress_bar.set_style(
            ProgressStyle::default_spinner().template("{prefix:>12.bold.cyan} {spinner} {msg}"),
        );
        self.process_scripts_folder(
            &config,
            &path,
            &path,
            &path_out_scripts,
            progress_bar.clone(),
        )?;
        progress_bar.finish_and_clear();
        let elapsed = started.elapsed();
        println!(
            "{:>12} scripts in {}.{}s",
            style("Finished").green().bold(),
            elapsed.as_secs(),
            elapsed.subsec_millis(),
        );
        Ok(())
    }

    pub fn process_res(&self, config: &LoadConfig, path: &Path) -> Res<()> {
        for entry in read_dir(path).expect("iter files in input") {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    if name.eq_ignore_ascii_case("cdclient.fdb") {
                        if self.opt.tables {
                            let path = entry.path();
                            self.run_cdc(&config, &path)?;
                        }
                    } else if self.opt.scripts && name.eq_ignore_ascii_case("scripts") {
                        let path = entry.path();
                        self.process_scripts(&config, &path)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn process_locale(&self, config: &LoadConfig, path: &Path) -> Res<()> {
        for entry in read_dir(path).expect("iter files in input") {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    if name.eq_ignore_ascii_case("locale.xml") {
                        let path = entry.path();
                        self.process_locale_xml(&config, &path)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn run(self) -> EmptyResult {
        let config = self.load_config().with_context(|| "Loading config")?;

        for entry in read_dir(&self.opt.prefix).expect("iter files in input") {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    if name.eq_ignore_ascii_case("res") {
                        let path = entry.path();
                        self.process_res(&config, &path)?;
                    } else if self.opt.locale && name.eq_ignore_ascii_case("locale") {
                        let path = entry.path();
                        self.process_locale(&config, &path)?;
                    }
                }
            }
        }

        // Tell the IO thread that we are done
        self.sender.send(None)?;
        print!("Waiting for IO... ");
        io::stdout().flush().expect("Could not flush stdout");

        match self.join_handle.join() {
            Ok((count, elapsed)) => {
                println!(
                    "\r{:>12} IO in {}.{}s ({} files)",
                    style("Finished").green().bold(),
                    elapsed.as_secs(),
                    elapsed.subsec_millis(),
                    count,
                );
                Ok(())
            }
            Err(_) => Err(anyhow!("IO failed!")),
        }
    }

    pub fn new(opt: WrenchOpt) -> Self {
        let (sender, r) = crossbeam_channel::unbounded();

        let join_handle = thread::spawn(move || {
            let mut count = 0;
            let start = Instant::now();
            while let Some((path, contents)) = r.recv().unwrap() {
                if let Err(e) = std::fs::write(path, contents) {
                    log::error!("{}", e);
                }
                count += 1;
            }

            let end = Instant::now();
            (count, end.duration_since(start))
        });

        Self {
            opt,
            sender,
            join_handle,
        }
    }
}
