use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub entries: Vec<Entry>,
    pub next_node: u32,
    pub leaf: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub index: IndexValue,
    pub left_ref: Option<u32>,
    pub right_ref: u32,
}

impl Ord for Entry {
    fn cmp(&self, other: &Entry) -> Ordering {
        return self.index.cmp(&other.index);
    }
}

impl Eq for Entry {}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum IndexValue {
    Bool(bool),
    Integer(isize),
    String(String),
    Array(Vec<IndexValue>),
}

impl fmt::Debug for IndexValue {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IndexValue::Bool(v) => formatter.debug_tuple("Bool").field(&v).finish(),
            IndexValue::Integer(ref v) => fmt::Debug::fmt(v, formatter),
            IndexValue::String(ref v) => formatter.debug_tuple("String").field(v).finish(),
            IndexValue::Array(ref v) => {
                formatter.write_str("Array(")?;
                fmt::Debug::fmt(v, formatter)?;
                formatter.write_str(")")
            }
        }
    }
}
