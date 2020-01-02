use {
    anyhow::{Context, Error},
    assembly::fdb::{
        builder::DatabaseBuilder,
        core::Field,
        file::FDBTableHeader,
        reader::{DatabaseBufReader, DatabaseReader},
    },
    miniserde::{
        json::{self, Array, Object, Value},
        ser::Fragment,
        Deserialize, Serialize,
    },
    std::fs::{read_dir, File},
    std::{
        borrow::Cow,
        collections::BTreeMap,
        //error::Error,
        io::{BufReader, BufWriter, Read, Write},
        path::{Path, PathBuf},
    },
    structopt::StructOpt,
};

#[derive(StructOpt)]
pub struct WrenchOpt {
    /// The base `res` folder of a LU installation
    prefix: PathBuf,
    /// The directory to place `lu-json` into.
    #[allow(unused)]
    output: PathBuf,
    /// The path to the configuration json
    config: PathBuf,
}

type Res<T> = Result<T, Error>;
type EmptyResult = Res<()>;

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    level: u8,
    index: Option<BTreeMap<String, u8>>,
    many: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoadConfig {
    tables: BTreeMap<String, TableConfig>,
}

#[derive(Debug, Serialize)]
pub struct Rel {
    href: String,
}

#[derive(Debug, Serialize)]
pub struct HAL<T> {
    _links: BTreeMap<&'static str, Rel>,
    _embedded: T,
}

//#[derive(Debug, Serialize)]
pub type SimpleTable<'a> = Vec<BTreeMap<&'a str, FW>>;

#[derive(Debug)]
pub struct FW(Field);

impl Serialize for FW {
    fn begin(&self) -> Fragment {
        match &self.0 {
            Field::Nothing => Fragment::Null,
            Field::Integer(i) => Fragment::I64(i64::from(*i)),
            Field::Float(f) => Fragment::F64(f64::from(*f)),
            Field::Text(s) => Fragment::Str(Cow::from(s)),
            Field::Boolean(b) => Fragment::Bool(*b),
            Field::BigInt(i) => Fragment::I64(*i),
            Field::VarChar(s) => Fragment::Str(Cow::from(s)),
        }
    }
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

    fn run_store_table<T: DatabaseBufReader>(
        &self,
        loader: &mut T,
        config: &LoadConfig,
        path_tables: &Path,
        th: &FDBTableHeader,
    ) -> EmptyResult {
        let tdh = loader.get_table_def_header(th.table_def_header_addr)?;
        let name = loader.get_string(tdh.table_name_addr)?;
        let tth = loader.get_table_data_header(th.table_data_header_addr)?;

        if let Some(t) = config.tables.get(&name) {
            println!("=== {} ===", name);
            //println!("{:?}", t);

            let chl = loader.get_column_header_list(&tdh)?;
            let chlv: Vec<_> = chl.into();

            let cnr: Result<Vec<String>, _> = chlv
                .iter()
                .map(|ch| loader.get_string(ch.column_name_addr))
                .collect();

            let column_names = cnr?;

            //println!("{:?}", column_names);
            let path_table = path_tables.clone().join(&name);
            let path_table_index = path_table.clone().join("index.json");
            println!("{}", path_table.display());

            match t.level {
                0 => {
                    // Store to a single json file
                    let j_single_index = {
                        let mut _links = BTreeMap::new();
                        let href = String::from("/") + &path_table_index.to_string_lossy();
                        _links.insert("self", Rel { href });
                        let mut _embedded = BTreeMap::new();
                        _embedded.insert(&name, {
                            let mut arr = Vec::new();

                            let _bc = tth.bucket_count;
                            let bhlv: Vec<_> = loader.get_bucket_header_list(&tth)?.into();

                            for bh in bhlv {
                                let rhlha = bh.row_header_list_head_addr;

                                let rhi = loader.get_row_header_addr_iterator(rhlha);
                                for orha in rhi.collect::<Vec<_>>() {
                                    let rha = orha?;
                                    let rh = loader.get_row_header(rha)?;

                                    let mut obj = BTreeMap::new();

                                    let fdlv: Vec<_> = loader.get_field_data_list(rh)?.into();
                                    for (i, fd) in fdlv.iter().enumerate() {
                                        let f = loader.try_load_field(&fd)?;
                                        obj.insert(&column_names[i], FW(f));
                                    }

                                    arr.push(obj);
                                }
                            }
                            arr
                        });
                        HAL { _links, _embedded }
                    };

                    let out = json::to_string(&j_single_index);
                    let p_out = self.output.join(path_table_index);

                    std::fs::create_dir_all(p_out.parent().unwrap())?;
                    let f_out = File::create(p_out)?;
                    let mut b_out = BufWriter::new(f_out);
                    write!(b_out, "{}", out)?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn run_cdc(&self, path: &Path) -> EmptyResult {
        let config = self.load_config().with_context(|| "Loading config")?;

        let file = File::open(path)?;
        let mut loader = BufReader::new(file);

        let h = loader.get_header()?;
        let thl = loader.get_table_header_list(h)?;

        let thlv: Vec<_> = thl.into();
        let iter = thlv.iter();

        let path = Path::new("lu-json");
        let path_tables = path.clone().join("tables");

        for th in iter {
            self.run_store_table(&mut loader, &config, &path_tables, &th)?;
        }

        Ok(())
    }

    pub fn run(&self) -> EmptyResult {
        for entry in read_dir(&self.prefix).expect("iter files in input") {
            if let Ok(entry) = entry {
                if let Some(name) = entry.file_name().to_str() {
                    if name.eq_ignore_ascii_case("cdclient.fdb") {
                        let path = entry.path();
                        self.run_cdc(&path)?;
                    }
                }
            }
        }

        Ok(())
    }
}
