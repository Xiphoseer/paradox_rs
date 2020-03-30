use {
    assembly::fdb::core::Field,
    miniserde::{
        json::{self, Array, Number, Object, Value},
        ser::{Fragment, Map},
        Serialize,
    },
    std::{borrow::Cow, collections::BTreeMap},
};

#[derive(Debug, Serialize)]
pub struct Rel {
    pub href: String,
}

#[derive(Debug, Serialize)]
pub struct HAL<T> {
    pub _links: BTreeMap<&'static str, Rel>,
    pub _embedded: T,
}

#[derive(Debug)]
pub struct IndexEntry<'a> {
    pub links: BTreeMap<&'static str, Rel>,
    pub fields: BTreeMap<&'a str, FW>,
}

pub struct ManyEntry<'a> {
    pub entries: Vec<Row<'a>>,
    pub entries_key: &'a str,
    pub index: i32,
    pub index_key: &'a str,
}

pub struct ManyEntryStream<'a, 'b> {
    state: u8,
    data: &'b ManyEntry<'a>,
}

impl<'a> Serialize for ManyEntry<'a> {
    fn begin(&self) -> Fragment {
        Fragment::Map(Box::new(ManyEntryStream {
            state: 0,
            data: self,
        }))
    }
}

impl<'a, 'b> Map for ManyEntryStream<'a, 'b> {
    fn next(&mut self) -> Option<(Cow<str>, &dyn Serialize)> {
        match self.state {
            0 => {
                self.state = 1;
                Some((Cow::Borrowed(self.data.index_key), &self.data.index))
            }
            1 => {
                self.state = 2;
                Some((Cow::Borrowed(self.data.entries_key), &self.data.entries))
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    pub fields: Vec<(&'a str, Value)>,
}

impl<'a> Row<'a> {
    pub fn insert(&mut self, col: &'a str, data: Value) {
        let mut p = col.splitn(2, '_');
        let first = p.next().unwrap();
        if let Some(next) = p.next() {
            match self.fields.last_mut() {
                Some(last) if last.0 == first => {
                    if let Value::Object(o) = &mut last.1 {
                        o.insert(next.to_string(), data);
                    } else {
                        panic!("Could not insert `{}` at `{}`", json::to_string(&data), col);
                    }
                }
                _ => {
                    let mut obj = Object::new();
                    obj.insert(next.to_string(), data);
                    self.fields.push((first, Value::Object(obj)));
                }
            }
        } else {
            self.fields.push((col, data));
        }
    }

    pub fn new(column_count: usize) -> Self {
        Self {
            fields: Vec::with_capacity(column_count),
        }
    }
}

pub struct RowStream<'a, 'b> {
    state: std::slice::Iter<'b, (&'a str, Value)>,
}

impl<'a> Serialize for Row<'a> {
    fn begin(&self) -> Fragment {
        Fragment::Map(Box::new(RowStream {
            state: self.fields.iter(),
        }))
    }
}

impl<'a, 'b> Map for RowStream<'a, 'b> {
    fn next(&mut self) -> Option<(Cow<str>, &dyn Serialize)> {
        self.state
            .next()
            .map(|(n, f)| (Cow::from(*n), f as &dyn Serialize))
    }
}

#[derive(Debug)]
pub struct Entry<'a> {
    links: &'a BTreeMap<&'static str, Rel>,
    fields: BTreeMap<&'a str, FW>,
}

impl<'a> Serialize for IndexEntry<'a> {
    fn begin(&self) -> Fragment {
        Fragment::Map(Box::new(EntryStream {
            links: Some(&self.links),
            state: self.fields.iter(),
        }))
    }
}

impl<'a> Serialize for Entry<'a> {
    fn begin(&self) -> Fragment {
        Fragment::Map(Box::new(EntryStream {
            links: Some(self.links),
            state: self.fields.iter(),
        }))
    }
}

pub struct EntryStream<'a, 'b> {
    links: Option<&'b BTreeMap<&'static str, Rel>>,
    state: std::collections::btree_map::Iter<'b, &'a str, FW>,
}

impl<'a, 'b> Map for EntryStream<'a, 'b> {
    fn next(&mut self) -> Option<(Cow<str>, &dyn Serialize)> {
        if let Some(links) = self.links.take() {
            Some((Cow::Borrowed("_links"), links))
        } else {
            self.state
                .next()
                .map(|(n, f)| (Cow::from(*n), f as &dyn Serialize))
        }
    }
}

#[derive(Debug)]
pub struct FW(pub Field);

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

pub trait IntoValue {
    fn to_json(self) -> Value;
    fn integer(&self) -> Option<i32>;
    fn string(&self) -> Option<&str>;
    fn to_key(&self) -> Cow<str>;
}

impl IntoValue for Field {
    fn to_json(self) -> Value {
        match self {
            Field::Nothing => Value::Null,
            Field::Integer(i) => Value::Number(Number::I64(i64::from(i))),
            Field::Float(f) => Value::Number(Number::F64(f64::from(f))),
            Field::Text(s) => Value::String(s),
            Field::Boolean(b) => Value::Bool(b),
            Field::BigInt(i) => Value::Number(Number::I64(i)),
            Field::VarChar(s) => Value::String(s),
        }
    }

    fn integer(&self) -> Option<i32> {
        if let Field::Integer(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    fn string(&self) -> Option<&str> {
        if let Field::Text(t) = self {
            Some(t)
        } else {
            None
        }
    }

    fn to_key(&self) -> Cow<str> {
        match self {
            Field::Nothing => Cow::Borrowed(""),
            Field::Integer(i) => Cow::Owned(i.to_string()),
            Field::Float(_f) => panic!("Don't use floats as keys"),
            Field::Text(s) => Cow::Borrowed(s),
            Field::Boolean(b) => {
                if *b {
                    Cow::Borrowed("true")
                } else {
                    Cow::Borrowed("false")
                }
            }
            Field::BigInt(i) => Cow::Owned(i.to_string()),
            Field::VarChar(s) => Cow::Borrowed(s),
        }
    }
}

pub trait MapExt {
    fn map(self, mapper: &str) -> Self;
}

impl MapExt for Value {
    fn map(self, mapper: &str) -> Self {
        match mapper {
            "list" => match self {
                Value::String(list) => {
                    if list.is_empty() {
                        Value::Array(Array::new())
                    } else {
                        Value::Array(
                            list.split(",")
                                .map(|s| Value::String(s.trim().to_owned()))
                                .collect(),
                        )
                    }
                }
                Value::Null => Value::Array(Array::new()),
                _ => {
                    println!(
                        "map: list: value is not a list (`{}`)",
                        json::to_string(&self)
                    );
                    self
                }
            },
            _ => {
                panic!("map: unknown mapper `{}`", mapper);
            }
        }
    }
}
