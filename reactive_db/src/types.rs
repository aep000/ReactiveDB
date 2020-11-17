use crate::config::parser::Tokens;
use crate::IndexValue;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::io::{Error, ErrorKind};
use uuid::Uuid;

pub type Entry = BTreeMap<String, EntryValue>;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Serialize, Deserialize, Debug)]
pub enum DataType {
    Integer,
    Array(Box<DataType>),
    Map(Vec<(String, DataType)>),
    ID,
    Float,
    Str,
    Bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialOrd, PartialEq, Ord)]
pub enum EntryValue {
    Integer(isize),
    Array(Vec<EntryValue>),
    Map(Entry),
    //Float(f64),
    Str(String),
    Bool(bool),
    ID(String)
}

impl DataType {
    pub fn is_indexible(&mut self) -> bool {
        match self {
            DataType::Integer => true,
            DataType::Array(dt) => dt.is_indexible(),
            DataType::Map(_) => false,
            DataType::Float => false,
            DataType::Str => true,
            DataType::Bool => true,
            DataType::ID => true,
        }
    }
}

impl EntryValue {
    pub fn to_index_value(&self) -> io::Result<IndexValue> {
        match self {
            EntryValue::Integer(v) => Ok(IndexValue::Integer(*v)),
            EntryValue::Array(val) => {
                let mut output = vec![];
                for part in val {
                    output.push(part.to_index_value()?);
                }
                Ok(IndexValue::Array(output))
            }
            EntryValue::Str(v) => Ok(IndexValue::String(v.clone())),
            EntryValue::ID(v) => Ok(IndexValue::ID(v.clone())),
            others => Err(create_custom_io_error(
                format!("Error Converting {:?} to IndexValue", others).as_str(),
            )),
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Ord, PartialOrd)]
pub enum Comparison {
    Lt,
    Gt,
    Gte,
    Lte,
    Eq,
    Neq,
    Or,
    And,
}

impl Comparison {
    fn evaluate(&self, left: EntryValue, right: EntryValue) -> Result<EntryValue, String> {
        match (left, right) {
            (EntryValue::Integer(x), EntryValue::Integer(y)) => {
                Ok(self.does_ordering_pass(x.cmp(&y)))
            }
            (EntryValue::Str(x), EntryValue::Str(y)) => Ok(self.does_ordering_pass(x.cmp(&y))),
            (EntryValue::Array(x), EntryValue::Array(y)) => Ok(self.does_ordering_pass(x.cmp(&y))),
            (EntryValue::Bool(x), EntryValue::Bool(y)) => match self {
                Comparison::And => Ok(EntryValue::Bool(x && y)),
                Comparison::Or => Ok(EntryValue::Bool(x || y)),
                Comparison::Eq => Ok(EntryValue::Bool(x == y)),
                Comparison::Neq => Ok(EntryValue::Bool(x != y)),
                _ => Err(format!(
                    "Unable to do operation {:?} on {:?} and {:?}",
                    self, x, y
                )),
            },
            (left, right) => Err(format!(
                "Unable to do operation {:?} on {:?} and {:?}",
                self, left, right
            )),
        }
    }
    fn does_ordering_pass(&self, ord: Ordering) -> EntryValue {
        let t = EntryValue::Bool(true);
        let f = EntryValue::Bool(false);
        match (self, ord) {
            (Comparison::Lt, Ordering::Less) => t,
            (Comparison::Gt, Ordering::Greater) => t,
            (Comparison::Gte, Ordering::Greater) => t,
            (Comparison::Gte, Ordering::Equal) => t,
            (Comparison::Lte, Ordering::Less) => t,
            (Comparison::Lte, Ordering::Equal) => t,
            (Comparison::Eq, Ordering::Equal) => t,
            (Comparison::Neq, Ordering::Less) => t,
            (Comparison::Neq, Ordering::Greater) => t,
            _ => f,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Ord, PartialOrd)]
pub enum Operation {
    Mult,
    Div,
    Add,
    Sub,
    Exp,
}

impl Operation {
    fn evaluate(&self, left: EntryValue, right: EntryValue) -> Result<EntryValue, String> {
        match (left, right) {
            (EntryValue::Integer(x), EntryValue::Integer(y)) => match self {
                Operation::Mult => Ok(EntryValue::Integer(x * y)),
                Operation::Div => Ok(EntryValue::Integer(x / y)),
                Operation::Add => Ok(EntryValue::Integer(x + y)),
                Operation::Sub => Ok(EntryValue::Integer(x - y)),
                Operation::Exp => Ok(EntryValue::Integer(x.pow(y.abs() as u32))),
            },
            (EntryValue::Str(x), EntryValue::Str(y)) => match self {
                Operation::Add => {
                    let mut out = x.to_owned();
                    out.push_str(y.as_str());
                    return Ok(EntryValue::Str(x));
                }
                _ => Err("Only add is a valid string operation".to_string()),
            },
            (left, right) => Err(format!(
                "Unable to do operation {:?} on {:?} and {:?}",
                self, left, right
            )),
        }
    }
}

#[derive(Debug, Ord, Eq, PartialOrd, PartialEq, Clone)]
pub enum OperationOrComparison {
    Operation(Operation),
    Comparison(Comparison),
}

impl OperationOrComparison {
    pub fn get_operation_or_comparison(token: Tokens) -> Result<OperationOrComparison, ()> {
        match token {
            Tokens::Operator(operation) => Ok(OperationOrComparison::Operation(operation)),
            Tokens::Comparison(comparison) => Ok(OperationOrComparison::Comparison(comparison)),
            _ => Err(()),
        }
    }
    pub fn evaluate(&self, left: EntryValue, right: EntryValue) -> Result<EntryValue, String> {
        match self {
            OperationOrComparison::Operation(op) => op.evaluate(left, right),
            OperationOrComparison::Comparison(cmp) => cmp.evaluate(left, right),
        }
    }
}

pub fn create_custom_io_error(text: &str) -> Error {
    return Error::new(ErrorKind::Other, text);
}
