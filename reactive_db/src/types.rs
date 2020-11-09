use std::io::{Error, ErrorKind};
use crate::IndexValue;
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use std::io;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Serialize, Deserialize, Debug)]
pub enum DataType {
    Integer,
    Array(Box<DataType>),
    Map(Vec<(String, DataType)>),
    Float,
    Str,
    Bool
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EntryValue{
    Integer(isize),
    Array(Vec<EntryValue>),
    Map(BTreeMap<String, EntryValue>),
    Float(f64),
    Str(String),
    Bool(bool)
}

impl DataType {
    pub fn is_indexible(&mut self) -> bool{
        match self {
            DataType::Integer => true,
            DataType::Array(dt) => dt.is_indexible(),
            DataType::Map(_) => false,
            DataType::Float => false,
            DataType::Str => true,
            DataType::Bool => true
        }
    }
}

impl EntryValue {
    pub fn to_index_value(&self) -> io::Result<IndexValue>{
        match self {
            EntryValue::Integer(v) => Ok(IndexValue::Integer(*v)),
            EntryValue::Array(val) => {
                let mut output = vec![];
                for part in val{
                    output.push(part.to_index_value()?);
                }
                Ok(IndexValue::Array(output))
            },
            EntryValue::Str(v) => Ok(IndexValue::String(v.clone())),
            others => Err(create_custom_io_error(format!("Error Converting {:?} to IndexValue", others).as_str()))
        }
    }
}

pub fn create_custom_io_error(text:&str) -> Error{
    return Error::new(ErrorKind::Other, text);
}
