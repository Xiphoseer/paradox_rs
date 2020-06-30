use {
    anyhow::anyhow,
    anyhow::{Context, Error},
    assembly::fdb::{
        builder::DatabaseBuilder,
        core::ValueType,
        file::{FDBBucketHeader, FDBFieldData, FDBTableDataHeader, FDBTableHeader},
        reader::{DatabaseBufReader, DatabaseReader},
    },
    console::style,
    encoding_rs::WINDOWS_1252,
    encoding_rs_io::DecodeReaderBytesBuilder,
    full_moon_compat_luaparse::Chunk,
    indicatif::{ProgressBar, ProgressStyle},
    miniserde::{json, Serialize},
    quick_xml::{
        events::{BytesEnd as XmlBytesEnd, BytesStart as XmlBytesStart, Event as XmlEvent},
        Reader as XmlReader,
    },
    std::{
        collections::{btree_map::Entry as BTreeEntry, BTreeMap},
        error::Error as StdError,
        fs::{read_dir, File},
        io::{BufRead, BufReader, BufWriter, Read, Write},
        path::{Path, PathBuf},
        str::FromStr,
        time::Instant,
    },
    structopt::StructOpt,
};

pub mod ser;
use ser::*;
pub mod de;
use de::*;
pub mod data;
use data::{Filter, GroupBy};

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

type Res<T> = Result<T, Error>;
type EmptyResult = Res<()>;

fn unpaged_suffix(id: i32, path_entry: &mut PathBuf) {
    path_entry.push(&format!("{}.json", id));
}

fn paged_suffix(id: i32, path_entry: &mut PathBuf) {
    let page = id / 256;
    path_entry.push(&format!("{}/{}.json", page, id));
}

fn dblpaged_suffix(id: i32, path_entry: &mut PathBuf) {
    let page_a = id / 256;
    let page_b = page_a / 256;
    path_entry.push(&format!("{}/{}/{}.json", page_b, page_a, id));
}

fn get_index_fields<'c, T: DatabaseBufReader>(
    loader: &mut T,
    indexer: &'c BTreeMap<String, usize>,
    fdlv: &Vec<FDBFieldData>,
) -> Res<BTreeMap<&'c str, FW>> {
    let mut index_fields = BTreeMap::new();
    for (col_name, col_index) in indexer {
        let fd = &fdlv[usize::from(*col_index)];
        let f = loader.try_load_field(fd)?;

        index_fields.insert(col_name.as_str(), FW(f));
    }
    Ok(index_fields)
}

fn links_for_self(path_entry: &Path) -> BTreeMap<&'static str, Rel> {
    let mut links = BTreeMap::new();
    let href = String::from("/") + &path_entry.to_string_lossy();
    links.insert("self", Rel { href });
    links
}

fn setup_group_by<'a>(
    group_by: &'a Option<BTreeMap<String, Vec<String>>>,
    column_names: &'a [String],
    column_types: &'a [ValueType],
) -> Vec<GroupBy<'a>> {
    let mut vec = Vec::new();
    if let Some(groups) = group_by {
        for (group_name, fields) in groups {
            match &fields[..] {
                [field] => {
                    let field_str = field.as_str();
                    if let Some(field_index) = column_names
                        .iter()
                        .position(|col_name| col_name == field_str)
                    {
                        if ValueType::Integer == column_types[field_index] {
                            vec.push(GroupBy::int_group(group_name, field, field_index));
                        } else {
                            vec.push(GroupBy::group(group_name, field, field_index));
                        }
                    } else {
                        panic!("Column `{}` not found!", field);
                    }
                }
                [outer, inner] => {
                    let outer_str = outer.as_str();
                    let inner_str = inner.as_str();
                    match column_names
                        .iter()
                        .fold((0, None, None), |(i, a, b), col_name| {
                            if col_name == outer_str {
                                (i + 1, Some(i), b)
                            } else if col_name == inner_str {
                                (i + 1, a, Some(i))
                            } else {
                                (i + 1, a, b)
                            }
                        }) {
                        (_, Some(outer_index), Some(inner_index)) => {
                            vec.push(GroupBy::group2(
                                group_name,
                                outer,
                                outer_index,
                                inner,
                                inner_index,
                            ));
                        }
                        (_, Some(_), None) => panic!("Column `{}` not found!", inner),
                        (_, None, Some(_)) => panic!("Column `{}` not found!", outer),
                        (_, None, None) => panic!("Columns (`{}`, `{}`) not found!", outer, inner),
                    }
                }
                _ => todo!(),
            }
        }
    }
    vec
}

impl WrenchOpt {
    fn load_config(&self) -> Res<LoadConfig> {
        let mut buf = String::new();
        let f_config = File::open(&self.config)?;
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
                return Err(anyhow!("Failed to write `{}`", file.display())).into();
            }
        };

        let p_out = self.output.join(file);

        std::fs::create_dir_all(p_out.parent().unwrap())?;
        let f_out = File::create(p_out)?;
        let mut b_out = BufWriter::new(f_out);
        write!(b_out, "{}", out)?;
        Ok(())
    }

    fn for_each_bucket<T: DatabaseBufReader, F>(
        &self,
        loader: &mut T,
        tth: &FDBTableDataHeader,
        mut fun: F,
    ) -> EmptyResult
    where
        F: FnMut(&mut T, &FDBBucketHeader) -> EmptyResult,
    {
        let bhlv: Vec<_> = loader.get_bucket_header_list(&tth)?.into();
        for bh in bhlv {
            fun(loader, &bh)?;
        }
        Ok(())
    }

    fn for_bucket_rows<T: DatabaseBufReader, F>(
        &self,
        loader: &mut T,
        bh: &FDBBucketHeader,
        mut fun: F,
    ) -> EmptyResult
    where
        F: FnMut(&mut T, &Vec<FDBFieldData>) -> EmptyResult,
    {
        let rhlha = bh.row_header_list_head_addr;

        let rhi = loader.get_row_header_addr_iterator(rhlha);
        for orha in rhi.collect::<Vec<_>>() {
            let rha = orha?;
            let rh = loader.get_row_header(rha)?;

            let fdlv: Vec<_> = loader.get_field_data_list(rh)?.into();
            fun(loader, &fdlv)?;
        }
        Ok(())
    }

    fn for_each_row<T: DatabaseBufReader, F>(
        &self,
        loader: &mut T,
        tth: &FDBTableDataHeader,
        mut fun: F,
    ) -> EmptyResult
    where
        F: FnMut(&mut T, &Vec<FDBFieldData>) -> EmptyResult,
    {
        self.for_each_bucket(loader, tth, move |loader, bh| {
            self.for_bucket_rows(loader, bh, |loader, fdlv| fun(loader, fdlv))
        })
    }

    fn run_store_table<T: DatabaseBufReader>(
        &self,
        loader: &mut T,
        config: &LoadConfig,
        path_tables: &Path,
        th: &FDBTableHeader,
        progress_bar: ProgressBar,
    ) -> EmptyResult {
        let tdh = loader.get_table_def_header(th.table_def_header_addr)?;
        let name = loader.get_string(tdh.table_name_addr)?;
        let tth = loader.get_table_data_header(th.table_data_header_addr)?;

        if let Some(t) = config.tables.get(&name) {
            progress_bar.set_message(&name);

            let chl = loader.get_column_header_list(&tdh)?;
            let chlv: Vec<_> = chl.into();

            let (column_names, column_types, column_prefix) = {
                let mut name_vec = Vec::with_capacity(chlv.len());
                let mut type_vec = Vec::with_capacity(chlv.len());
                let mut pref_vec = Vec::with_capacity(chlv.len());

                for ch in chlv {
                    let name_str = loader.get_string(ch.column_name_addr)?;
                    let data_type = ValueType::from(ch.column_data_type);

                    let mut prefix = None;
                    if let Some(p) = &t.prefix {
                        for (key, value) in p {
                            if name_str.starts_with(key) {
                                prefix = Some((value, name_str[key.len()..].to_string()));
                            }
                        }
                    }
                    if let Some(p) = &t.suffix {
                        for (key, value) in p {
                            if name_str.ends_with(key) {
                                let new_len = name_str.len() - key.len();
                                prefix = Some((value, name_str[..new_len].to_string()));
                            }
                        }
                    }

                    pref_vec.push(prefix);
                    name_vec.push(name_str);
                    type_vec.push(data_type);
                }

                (name_vec, type_vec, pref_vec)
            };

            // Set primary key
            let pk = t.pk.as_ref().map_or(0, |key| {
                column_names
                    .iter()
                    .position(|k| k.as_str() == key.as_str())
                    .expect("Invalid PK name!")
            });

            // Setup indexer
            let default_indexer = {
                let mut map = BTreeMap::new();
                map.insert("id".to_string(), pk);
                map
            };
            let indexer = t.index.as_ref().unwrap_or(&default_indexer);

            // Setup group by
            let mut group_by = setup_group_by(&t.group_by, &column_names, &column_types);

            // Setup mapper
            let default_mapper = BTreeMap::new();
            let mapper = t.map.as_ref().unwrap_or(&default_mapper);

            // Setup filters/ignores
            let ignore: Vec<Filter> = {
                let mut vec = Vec::new();
                if let Some(ignores) = &t.ignore {
                    for (col, filter) in ignores {
                        let pos = column_names
                            .iter()
                            .position(|n| n.as_str() == col.as_str())
                            .expect("Column not found");
                        match filter {
                            json::Value::String(string) => {
                                vec.push(Filter::string(pos, string)?);
                            }
                            _ => todo!(),
                        }
                    }
                }
                vec
            };

            let collect_row =
                |loader: &mut T, fdlv: &[FDBFieldData], pk: Option<usize>| -> Res<Row> {
                    let mut row = Row::new(tdh.column_count as usize);
                    for (i, fd) in fdlv.iter().enumerate() {
                        if Some(i) != pk {
                            let f = loader.try_load_field(&fd)?;
                            let c = column_names[i].as_str();
                            let mut v = f.to_json();
                            if let Some(m) = mapper.get(c) {
                                v = v.map(m);
                            }
                            if let Some((outer, inner)) = &column_prefix[i] {
                                if let Some((_k, value)) =
                                    row.fields.iter_mut().find(|(k, _)| k == outer)
                                {
                                    if let json::Value::Object(o) = value {
                                        o.insert(inner.to_string(), v);
                                    }
                                } else {
                                    let mut inner_map = json::Object::new();
                                    inner_map.insert(inner.to_string(), v);
                                    row.fields.push((outer, json::Value::Object(inner_map)));
                                }
                            } else {
                                row.fields.push((c, v));
                            }
                        }
                    }
                    Ok(row)
                };

            let path_table = {
                let mut path = path_tables.to_owned();
                for part in name.split("_") {
                    path.push(part);
                }
                path
            };
            let path_table_index = path_table.clone().join("index.json");

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
                            self.for_each_row(loader, &tth, |loader, fdlv| {
                                let obj = collect_row(loader, fdlv, None)?;
                                arr.push(obj);
                                Ok(())
                            })?;
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
                    let j_index = {
                        let mut links = BTreeMap::new();
                        let href = String::from("/") + &path_table_index.to_string_lossy();
                        links.insert("self", Rel { href });
                        let mut data = BTreeMap::new();

                        data.insert(&name, {
                            let mut arr = Vec::new();
                            if let Some(key) = &t.many {
                                self.for_each_bucket(loader, &tth, |loader, bh| {
                                    let mut bucket = BTreeMap::new();

                                    self.for_bucket_rows(loader, &bh, |loader, fdlv| {
                                        let f_id = loader.try_load_field(&fdlv[pk])?;
                                        let id = f_id.integer().expect("primary key not integer");

                                        let entry = bucket.entry(id);
                                        let j_entry = collect_row(loader, fdlv, Some(pk))?;
                                        match entry {
                                            BTreeEntry::Vacant(v_entry) => {
                                                let index_fields = get_index_fields(loader, indexer, &fdlv)?;
                                                v_entry.insert((index_fields, vec![j_entry]));
                                            }
                                            BTreeEntry::Occupied(mut o_entry) => {
                                                o_entry.get_mut().1.push(j_entry);
                                            }
                                        }
                                        Ok(())
                                    })?;

                                    for (id, (index_fields, entries)) in bucket {
                                        let mut path_entry = path_table.clone();
                                        paginator(id, &mut path_entry);
                                        let links = links_for_self(&path_entry);
                                        let entry = IndexEntry { links, fields: index_fields };
                                        arr.push(entry);

                                        let j_file = ManyEntry {
                                            index: id,
                                            index_key: column_names[pk].as_str(),
                                            entries,
                                            entries_key: key
                                        };
                                        self.make_file(&path_entry, &j_file)?;
                                    }

                                    Ok(())
                                })?;
                            } else {
                                self.for_each_row(loader, &tth, |loader, fdlv| {
                                    for filter in &ignore {
                                        if filter.check(loader, fdlv)? {
                                            // continue
                                            return Ok(())
                                        }
                                    }

                                    let f_id = loader.try_load_field(&fdlv[pk])?;
                                    let id = f_id.integer().expect("primary key not integer");

                                    let mut path_entry = path_table.clone();
                                    paginator(id, &mut path_entry);

                                    let index_fields = get_index_fields(loader, indexer, &fdlv)?;
                                    let links = links_for_self(&path_entry);

                                    let j_entry = collect_row(loader, fdlv, None)?;

                                    // Register with groups
                                    for g in &mut group_by[..] {
                                        g.collect(id, loader, &fdlv[..])?;
                                    }

                                    self.make_file(&path_entry, &j_entry)?;

                                    let entry = IndexEntry { links, fields: index_fields };
                                    arr.push(entry);

                                    Ok(())
                                })?;
                            }
                            arr
                        });
                        HAL {
                            _links: links,
                            _embedded: data,
                        }
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
        }

        Ok(())
    }

    pub fn process_locale_xml(&self, config: &LoadConfig, path: &Path) -> Res<()> {
        fn expect_start<'a, 'b, 'c, B: BufRead>(
            key: &'a str,
            reader: &'b mut XmlReader<B>,
            buf: &'c mut Vec<u8>,
        ) -> Res<XmlBytesStart<'c>> {
            if let Ok(XmlEvent::Start(e)) = reader.read_event(buf) {
                if e.name() == key.as_bytes() {
                    Ok(e)
                } else {
                    Err(anyhow!("Expected tag `{}`, found `{:?}`", key, e.name()))
                }
            } else {
                Err(anyhow!("Missing tag `{}`", key))
            }
        }

        fn expect_end<'a, 'b, 'c, B: BufRead>(
            key: &'a str,
            reader: &'b mut XmlReader<B>,
            buf: &'c mut Vec<u8>,
        ) -> Res<XmlBytesEnd<'c>> {
            if let Ok(XmlEvent::End(e)) = reader.read_event(buf) {
                if e.name() == key.as_bytes() {
                    Ok(e)
                } else {
                    Err(anyhow!(
                        "Expected end tag `{}`, found `{:?}`",
                        key,
                        e.name()
                    ))
                }
            } else {
                Err(anyhow!("Missing end tag `{}`", key))
            }
        }

        fn expect_text<'a, 'b, 'c, B: BufRead>(
            reader: &'b mut XmlReader<B>,
            buf: &'c mut Vec<u8>,
        ) -> Res<String> {
            if let Ok(XmlEvent::Text(e)) = reader.read_event(buf) {
                let text = e.unescape_and_decode(&reader)?;
                Ok(text)
            } else {
                Err(anyhow!("Missing text"))
            }
        }

        fn expect_attribute<T: FromStr, B: BufRead>(
            key: &str,
            reader: &XmlReader<B>,
            event: &XmlBytesStart,
        ) -> Res<T>
        where
            <T as FromStr>::Err: StdError + Send + Sync + 'static,
        {
            let attr = event
                .attributes()
                .next()
                .ok_or_else(|| anyhow!("Missing Attribute `{}`", key))??;

            if attr.key == key.as_bytes() {
                let attr_unesc = attr.unescaped_value()?;
                let attr_str = reader.decode(&attr_unesc)?;
                let value = attr_str.parse()?;
                Ok(value)
            } else {
                Err(anyhow!(
                    "Expected Attribute `{}`, found `{:?}`",
                    key,
                    attr.key
                ))
            }
        }

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
                    } else {
                        if let Some(nntbl) = table_next {
                            ntbl = nntbl;
                        } else {
                            table_current = None;
                            break;
                        }
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
                let page = table.entry(page_index).or_insert(BTreeMap::new());
                let row = page.entry(id).or_insert(BTreeMap::new());
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
        let mut loader = BufReader::new(file);

        let h = loader.get_header()?;
        let thl = loader.get_table_header_list(h)?;

        let thlv: Vec<_> = thl.into();

        let progress_bar = ProgressBar::new(thlv.len() as u64);
        progress_bar.set_prefix("Loading");
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{prefix:>12.bold.cyan} [{bar:55}] {pos}/{len}: {msg}")
                .progress_chars("=> "),
        );

        let iter = thlv.iter();

        let path = Path::new("lu-json");
        let path_tables = path.clone().join("tables");

        for th in iter {
            self.run_store_table(
                &mut loader,
                &config,
                &path_tables,
                &th,
                progress_bar.clone(),
            )?;
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
                } else if path.is_file() {
                    if path.extension() == Some(std::ffi::OsStr::new("lua")) {
                        let lua_file = std::fs::File::open(&path)?;
                        let lua_buf_reader = BufReader::new(lua_file);
                        let mut reader = DecodeReaderBytesBuilder::new()
                            .encoding(Some(WINDOWS_1252))
                            .build(lua_buf_reader);
                        let mut lua_text = String::new();
                        reader.read_to_string(&mut lua_text)?;

                        // scripts/02_client/map/am/l_draw_bridge_client.lua
                        // scripts/02_client/map/am/l_blue_x_client.lua
                        // scripts/02_client/map/am/l_skullkin_drill_client.lua
                        // scripts/02_client/map/property/ag_small/l_zone_ag_property_client.lua
                        // scripts/02_client/map/fv/l_fv_race_join_client.lua
                        // scripts/ai/wild/l_gp_control-vine.lua
                        // scripts/ai/minigame/objects/minigaeme_deathplane.lua
                        // scripts/ai/minigame/ag_battle/ag_battle_strike_maelstrom_effects_01.lua
                        // scripts/ai/minigame/siege/objects/loot_capture_object_client.lua
                        // scripts/ai/minigame/siege/client/seiege_timer_client.lua
                        // scripts/client/ai/np/l_np_statue_scene_1.lua

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
        }
        Ok(())
    }

    pub fn process_scripts(&self, config: &LoadConfig, path: &Path) -> Res<()> {
        let path_out = Path::new("lu-json");
        let path_out_scripts = path_out.clone().join("scripts");

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
                        if self.tables {
                            let path = entry.path();
                            self.run_cdc(&config, &path)?;
                        }
                    } else if name.eq_ignore_ascii_case("scripts") {
                        if self.scripts {
                            let path = entry.path();
                            self.process_scripts(&config, &path)?;
                        }
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

    pub fn run(&self) -> EmptyResult {
        let config = self.load_config().with_context(|| "Loading config")?;

        for entry in read_dir(&self.prefix).expect("iter files in input") {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    if name.eq_ignore_ascii_case("res") {
                        let path = entry.path();
                        self.process_res(&config, &path)?;
                    } else if name.eq_ignore_ascii_case("locale") {
                        if self.locale {
                            let path = entry.path();
                            self.process_locale(&config, &path)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
