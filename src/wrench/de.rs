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

#[derive(Debug, Deserialize)]
pub struct RefConfig {
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct LoadConfig {
    pub tables: BTreeMap<String, TableConfig>,
    pub refs: BTreeMap<String, RefConfig>,
}
