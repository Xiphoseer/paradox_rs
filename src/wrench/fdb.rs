use super::{
    data::{Filter, GroupBy},
    de::ColumnInfo,
    ser::{IndexEntry, IntoValue, ManyEntry, MapExt, Rel, Row, FW},
    Res,
};
use anyhow::anyhow;
use assembly::fdb::{
    align::{Row as RawRow, Table as RawTable},
    core::ValueType,
};
use derive_new::new;
use miniserde::json;
use std::{
    collections::{btree_map::Entry as BTreeEntry, BTreeMap},
    path::{Path, PathBuf},
};

pub fn unpaged_suffix(id: i32, path_entry: &mut PathBuf) {
    path_entry.push(&format!("{}.json", id));
}

pub fn paged_suffix(id: i32, path_entry: &mut PathBuf) {
    let page = id / 256;
    path_entry.push(&format!("{}/{}.json", page, id));
}

pub fn dblpaged_suffix(id: i32, path_entry: &mut PathBuf) {
    let page_a = id / 256;
    let page_b = page_a / 256;
    path_entry.push(&format!("{}/{}/{}.json", page_b, page_a, id));
}

pub fn get_index_fields<'b, 'c>(
    indexer: &'c BTreeMap<String, usize>,
    row: RawRow<'b>,
) -> Res<BTreeMap<&'c str, FW<'b>>> {
    let mut index_fields = BTreeMap::new();
    for (col_name, col_index) in indexer {
        let fd = row
            .field_at(*col_index)
            .ok_or_else(|| anyhow!("Index fields out of bounds"))?;

        index_fields.insert(col_name.as_str(), FW(fd));
    }
    Ok(index_fields)
}

pub fn links_for_self(path_entry: &Path) -> BTreeMap<&'static str, Rel> {
    let mut links = BTreeMap::new();
    let href = String::from("/") + &path_entry.to_string_lossy();
    links.insert("self", Rel { href });
    links
}

pub fn setup_group_by<'a>(
    group_by: &'a Option<BTreeMap<String, Vec<String>>>,
    columns: &'a [ColumnInfo<'a, '_>],
) -> Vec<GroupBy<'a>> {
    let mut vec = Vec::new();
    if let Some(groups) = group_by {
        for (group_name, fields) in groups {
            match &fields[..] {
                [field] => {
                    let field_str = field.as_str();
                    if let Some((field_index, col)) =
                        columns.iter().enumerate().find_map(|(index, col)| {
                            if col.name == field_str {
                                Some((index, col))
                            } else {
                                None
                            }
                        })
                    {
                        if ValueType::Integer == col.data_type {
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
                    match columns.iter().fold((0, None, None), |(i, a, b), col| {
                        if col.name == outer_str {
                            (i + 1, Some(i), b)
                        } else if col.name == inner_str {
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

#[derive(Copy, Clone, new)]
pub struct CollectRow<'a> {
    columns: &'a [ColumnInfo<'a, 'a>],
    mapper: &'a BTreeMap<String, String>,
}

impl<'a> CollectRow<'a> {
    pub(crate) fn invoke(self, raw_row: RawRow<'a>, pk: Option<usize>) -> Res<Row> {
        let mut row = Row::new(self.columns.len());
        for (i, fd) in raw_row.field_iter().enumerate() {
            if Some(i) != pk {
                //let f = loader.try_load_field(&fd)?;
                let c = self.columns[i].name.as_ref();
                let mut v = fd.to_json();
                if let Some(m) = self.mapper.get(c) {
                    v = v.map(m);
                }
                if let Some((outer, inner)) = &self.columns[i].prefix {
                    if let Some((_k, value)) = row.fields.iter_mut().find(|(k, _)| k == outer) {
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
    }
}

#[derive(new)]
pub struct StoreMulti<'a, 'b, 'c, M>
where
    M: Fn(&Path, &ManyEntry) -> Res<()>,
{
    pk: usize,
    key: &'a str,
    path_table: &'a PathBuf,
    paginator: fn(i32, &mut PathBuf),
    indexer: &'a BTreeMap<String, usize>,
    collect_row: &'b CollectRow<'a>,
    columns: &'a [ColumnInfo<'a, 'b>],
    group_by: &'c mut [GroupBy<'a>],
    make_file: M,
}

impl<'a, 'b, 'c, M> StoreMulti<'a, 'b, 'c, M>
where
    M: Fn(&Path, &ManyEntry) -> Res<()>,
{
    pub fn invoke(self, table: RawTable<'b>) -> Res<Vec<IndexEntry<'a, 'b>>> {
        let mut arr = Vec::new();
        for raw_bucket in table.bucket_iter() {
            let mut bucket = BTreeMap::new();

            for raw_row in raw_bucket.row_iter() {
                let f_id = raw_row
                    .field_at(self.pk)
                    .ok_or_else(|| anyhow!("Row without primary key at index {}", self.pk))?;
                let id = f_id
                    .integer()
                    .ok_or_else(|| anyhow!("Primary key not integer"))?;

                let entry = bucket.entry(id);
                let j_entry = self.collect_row.invoke(raw_row, Some(self.pk))?;
                match entry {
                    BTreeEntry::Vacant(v_entry) => {
                        let index_fields = get_index_fields(self.indexer, raw_row)?;
                        v_entry.insert((index_fields, vec![j_entry]));
                    }
                    BTreeEntry::Occupied(mut o_entry) => {
                        o_entry.get_mut().1.push(j_entry);
                    }
                }

                // Register with groups
                for g in &mut self.group_by[..] {
                    g.collect(id, raw_row)?;
                }
            }

            for (id, (index_fields, entries)) in bucket {
                let mut path_entry = self.path_table.clone();
                (self.paginator)(id, &mut path_entry);
                let links = links_for_self(&path_entry);
                let entry = IndexEntry {
                    links,
                    fields: index_fields,
                };
                arr.push(entry);

                let j_file = ManyEntry {
                    index: id,
                    index_key: self.columns[self.pk].name.as_ref(),
                    entries,
                    entries_key: self.key,
                };
                (self.make_file)(&path_entry, &j_file)?;
            }
        }
        Ok(arr)
    }
}

#[derive(new)]
pub struct StoreSimple<'a, 'b, M: Fn(&Path, &Row) -> Res<()>> {
    ignore: &'a [Filter<'a>],
    pk: usize,
    path_table: &'a PathBuf,
    paginator: fn(i32, &mut PathBuf),
    indexer: &'a BTreeMap<String, usize>,
    collect_row: CollectRow<'a>,
    group_by: &'b mut [GroupBy<'a>],
    make_file: M,
}

impl<'a, 'b, M: Fn(&Path, &Row) -> Res<()>> StoreSimple<'a, 'b, M> {
    pub(crate) fn invoke(mut self, table: RawTable<'b>) -> Res<Vec<IndexEntry<'a, 'b>>> {
        let mut vec = Vec::new();
        for row in table.row_iter() {
            if let Some(entry) = self.process_row(row)? {
                vec.push(entry);
            }
        }
        Ok(vec)
    }

    fn process_row(&mut self, row: RawRow<'b>) -> Res<Option<IndexEntry<'a, 'b>>> {
        for filter in self.ignore.iter() {
            if filter.check(row)? {
                // continue
                return Ok(None);
            }
        }

        let f_id = row
            .field_at(self.pk)
            .ok_or_else(|| anyhow!("PK field out of bounds"))?;
        let id = f_id
            .integer()
            .ok_or_else(|| anyhow!("primary key not integer"))?;

        let mut path_entry = self.path_table.clone();
        (self.paginator)(id, &mut path_entry);

        let index_fields = get_index_fields(self.indexer, row)?;
        let links = links_for_self(&path_entry);

        let j_entry = self.collect_row.invoke(row, None)?;

        // Register with groups
        for g in &mut self.group_by[..] {
            g.collect(id, row)?;
        }

        (self.make_file)(&path_entry, &j_entry)?;

        let entry = IndexEntry {
            links,
            fields: index_fields,
        };
        Ok(Some(entry))
    }
}
