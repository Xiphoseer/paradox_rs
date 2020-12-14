use super::{data::Filter, Res};
use anyhow::anyhow;
use assembly::fdb::{common::ValueType, mem::Table};
use miniserde::json;
use std::borrow::Cow;
use {
    miniserde::{json::Value, Deserialize},
    std::collections::BTreeMap,
};

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub level: u8,
    pub index: Option<BTreeMap<String, usize>>,
    pub map: Option<BTreeMap<String, String>>,
    pub many: Option<String>,
    pub group_by: Option<BTreeMap<String, Vec<String>>>,
    pub ignore: Option<BTreeMap<String, Value>>,
    pub prefix: Option<BTreeMap<String, String>>,
    pub suffix: Option<BTreeMap<String, String>>,
    pub pk: Option<String>,
}

pub struct ColumnInfo<'a, 't> {
    pub name: Cow<'a, str>,
    pub data_type: ValueType,
    pub prefix: Option<(&'t str, Cow<'a, str>)>,
}

pub trait CowStrExt: Sized {
    fn map_opt<'a, F>(&self, map: F) -> Option<Self>
    where
        F: for<'r> FnOnce(&'r str) -> Option<&'r str>;
}

impl<'a> CowStrExt for Cow<'a, str> {
    fn map_opt<F>(&self, map: F) -> Option<Self>
    where
        F: for<'r> FnOnce(&'r str) -> Option<&'r str>,
    {
        match self {
            Cow::Owned(v) => map(&v).map(str::to_owned).map(Cow::Owned),
            Cow::Borrowed(r) => map(*r).map(Cow::Borrowed),
        }
    }
}

impl TableConfig {
    pub fn setup_filters<'a>(&'a self, columns: &[ColumnInfo]) -> Res<Vec<Filter<'a>>> {
        let mut vec = Vec::new();
        if let Some(ignores) = &self.ignore {
            for (value, filter) in ignores {
                let pos = columns
                    .iter()
                    .position(|col| col.name == value.as_str())
                    .ok_or_else(|| anyhow!("Column not found"))?;
                match filter {
                    json::Value::String(string) => {
                        vec.push(Filter::string(pos, string)?);
                    }
                    _ => todo!(),
                }
            }
        }
        Ok(vec)
    }

    pub fn make_column_vecs<'a, 't>(&'t self, table: Table<'a>) -> Res<Vec<ColumnInfo<'a, 't>>> {
        let mut res = Vec::with_capacity(table.column_count());

        for column in table.column_iter() {
            let name = column.name();
            let data_type = column.value_type();

            let mut prefix = None;
            if let Some(p) = &self.prefix {
                for (key, value) in p {
                    if let Some(p) = name.map_opt(|n| n.strip_prefix(key)) {
                        prefix = Some((value.as_str(), p));
                    }
                }
            }
            if let Some(p) = &self.suffix {
                for (key, value) in p {
                    if let Some(s) = name.map_opt(|n| n.strip_suffix(key)) {
                        prefix = Some((value.as_str(), s));
                    }
                }
            }

            res.push(ColumnInfo {
                name,
                data_type,
                prefix,
            });
        }

        Ok(res)
    }
}

#[derive(Debug, Deserialize)]
pub struct RefConfig {
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct LoadConfig {
    pub tables: BTreeMap<String, TableConfig>,
    pub refs: BTreeMap<String, RefConfig>,
}
