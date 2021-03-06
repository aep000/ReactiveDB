use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub entries: Vec<NodeEntry>,
    pub next_node: u32,
    pub leaf: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeEntry {
    pub index: IndexValue,
    pub right_ref: u32,
    pub left_ref: Option<u32>,
}

impl Ord for NodeEntry {
    fn cmp(&self, other: &NodeEntry) -> Ordering {
        return self.index.cmp(&other.index);
    }
}

impl Eq for NodeEntry {}

impl PartialEq for NodeEntry {
    fn eq(&self, other: &NodeEntry) -> bool {
        self.index.cmp(&other.index) == Ordering::Equal
    }
}

impl PartialOrd for NodeEntry {
    fn partial_cmp(&self, other: &NodeEntry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum IndexValue {
    Bool(bool),
    Integer(isize),
    String(String),
    Array(Vec<IndexValue>),
    ID(String),
    Decimal(Decimal)
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
            IndexValue::ID(ref id) => fmt::Debug::fmt(id, formatter),
            IndexValue::Decimal(ref v) => fmt::Debug::fmt(v, formatter),
        }
    }
}
