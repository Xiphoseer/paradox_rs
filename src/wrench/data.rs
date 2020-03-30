use super::ser::IntoValue;
use super::{EmptyResult, Res};
use assembly::fdb::{builder::DatabaseBuilder, file::FDBFieldData};
use miniserde::Serialize;
use std::{collections::btree_map::BTreeMap, ops::Deref};

pub struct Filter<'a> {
    field_index: usize,
    kind: FilterKind<'a>,
}

pub enum FilterKind<'a> {
    StringStartsWith(&'a str),
    StringEndsWith(&'a str),
    StringContains(&'a str),
    StringExact(&'a str),
}

impl<'a> Filter<'a> {
    pub fn string(field_index: usize, pattern: &'a str) -> Res<Self> {
        match &pattern.split("%").collect::<Vec<_>>()[..] {
            [] => unreachable!(),
            [exact] => Ok(Self {
                field_index,
                kind: FilterKind::StringExact(exact),
            }),
            ["", ends_with] => Ok(Self {
                field_index,
                kind: FilterKind::StringEndsWith(ends_with),
            }),
            [starts_with, ""] => Ok(Self {
                field_index,
                kind: FilterKind::StringStartsWith(starts_with),
            }),
            _ => todo!(),
        }
    }

    pub fn check<T: DatabaseBuilder>(&self, loader: &mut T, fields: &[FDBFieldData]) -> Res<bool> {
        let value = loader.try_load_field(&fields[self.field_index])?;
        match &self.kind {
            FilterKind::StringStartsWith(needle) => {
                if let Some(s) = value.string() {
                    Ok(s.starts_with(needle))
                } else {
                    Ok(false)
                }
            }
            FilterKind::StringEndsWith(needle) => {
                if let Some(s) = value.string() {
                    Ok(s.ends_with(needle))
                } else {
                    Ok(false)
                }
            }
            _ => todo!(),
        }
    }
}

pub struct GroupBy<'a> {
    pub group_name: &'a str,
    kind: GroupByKind<'a>,
}

pub enum GroupByKind<'a> {
    Group {
        field: &'a str,
        field_index: usize,
        data: BTreeMap<String, Vec<i32>>,
    },
    IntGroup {
        field: &'a str,
        field_index: usize,
        data: BTreeMap<i32, Vec<i32>>,
    },
    Group2 {
        outer: &'a str,
        inner: &'a str,
        outer_index: usize,
        inner_index: usize,
        data: BTreeMap<String, BTreeMap<String, Vec<i32>>>,
    },
}

impl<'a> GroupBy<'a> {
    pub fn result(&self) -> &dyn Serialize {
        match &self.kind {
            GroupByKind::Group2 { data, .. } => data,
            GroupByKind::Group { data, .. } => data,
            GroupByKind::IntGroup { data, .. } => data,
        }
    }

    pub fn collect<T: DatabaseBuilder>(
        &mut self,
        id: i32,
        loader: &mut T,
        fields: &[FDBFieldData],
    ) -> EmptyResult {
        match &mut self.kind {
            GroupByKind::Group {
                field_index, data, ..
            } => {
                let value = loader.try_load_field(&fields[*field_index])?;
                let value_cow = value.to_key();
                match data.get_mut(value_cow.deref()) {
                    None => {
                        data.insert(value_cow.into_owned(), vec![id]);
                    }
                    Some(vec) => {
                        vec.push(id);
                    }
                }
            }
            GroupByKind::IntGroup {
                field_index, data, ..
            } => {
                let value = loader.try_load_field(&fields[*field_index])?;
                let value_int = value.integer().unwrap_or(-1);
                // FIXME: use entry
                match data.get_mut(&value_int) {
                    None => {
                        data.insert(value_int, vec![id]);
                    }
                    Some(vec) => {
                        vec.push(id);
                    }
                }
            }
            GroupByKind::Group2 {
                outer_index,
                inner_index,
                data,
                ..
            } => {
                let outer_value = loader.try_load_field(&fields[*outer_index])?;
                let outer_str = outer_value.string().unwrap_or("");
                let inner_value = loader.try_load_field(&fields[*inner_index])?;
                let inner_str = inner_value.string().unwrap_or("");
                match data.get_mut(outer_str) {
                    None => {
                        let mut inner_map = BTreeMap::new();
                        inner_map.insert(inner_str.to_string(), vec![id]);
                        data.insert(outer_str.to_string(), inner_map);
                    }
                    Some(inner_map) => match inner_map.get_mut(inner_str) {
                        None => {
                            inner_map.insert(inner_str.to_string(), vec![id]);
                        }
                        Some(vec) => {
                            vec.push(id);
                        }
                    },
                }
            }
        }
        Ok(())
    }

    pub fn group2(
        group_name: &'a str,
        outer: &'a str,
        outer_index: usize,
        inner: &'a str,
        inner_index: usize,
    ) -> Self {
        Self {
            group_name,
            kind: GroupByKind::Group2 {
                outer,
                inner,
                outer_index,
                inner_index,
                data: BTreeMap::new(),
            },
        }
    }

    pub fn group(group_name: &'a str, field: &'a str, field_index: usize) -> Self {
        Self {
            group_name,
            kind: GroupByKind::Group {
                field,
                field_index,
                data: BTreeMap::new(),
            },
        }
    }

    pub fn int_group(group_name: &'a str, field: &'a str, field_index: usize) -> Self {
        Self {
            group_name,
            kind: GroupByKind::IntGroup {
                field,
                field_index,
                data: BTreeMap::new(),
            },
        }
    }
}
