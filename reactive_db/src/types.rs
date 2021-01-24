use crate::IndexValue;
use cpython::{PyObject, PythonObject, ToPyObject};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::io::{Error, ErrorKind};
use rust_decimal::Decimal;

pub type Entry = BTreeMap<String, EntryValue>;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Serialize, Deserialize, Debug, Hash)]
pub enum DataType {
    Integer,
    Array(Box<DataType>),
    Map(Vec<(String, DataType)>),
    ID,
    Float,
    Str,
    Bool,
    Decimal
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialOrd, PartialEq, Ord, Hash)]
pub enum EntryValue {
    Integer(isize),
    Array(Vec<EntryValue>),
    Map(Entry),
    //Float(f64),
    Str(String),
    Bool(bool),
    ID(String),
    Decimal(Decimal)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialOrd, PartialEq, Ord, Hash)]
pub struct DBEdit {
    pub table: String,
    pub edit_params: EditType
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialOrd, PartialEq, Ord, Hash)]
pub enum EditType {
    Insert(Entry),
    Delete(String, EntryValue),
    Update(Entry, String, EntryValue)
}


impl DBEdit {
    pub fn new(table: String, edit_params: EditType) -> DBEdit {
        DBEdit {
            table,
            edit_params: edit_params
        }
    }
}
#[derive(Debug, Clone)]
pub struct CommitedEdit {
    pub table: String,
    pub entry: Entry
}

impl CommitedEdit {
    pub fn new(table: String, entry: Entry) -> CommitedEdit{
        CommitedEdit {
            table,
            entry
        }
    }
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
            DataType::Decimal => true
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
            EntryValue::Decimal(v) => Ok(IndexValue::Decimal(v.clone())),
            others => Err(create_custom_io_error(
                format!("Error Converting {:?} to IndexValue", others).as_str(),
            )),
        }
    }
}

pub fn create_custom_io_error(text: &str) -> Error {
    return Error::new(ErrorKind::Other, text);
}

impl ToPyObject for EntryValue {
    type ObjectType = PyObject;

    fn to_py_object(&self, py: cpython::Python) -> Self::ObjectType {
        match self {
            EntryValue::Integer(value) => {
                value.to_py_object(py).as_object().into_py_object(py)
            }
            EntryValue::Array(value) => {
                value.to_py_object(py).as_object().into_py_object(py)
            }
            EntryValue::Map(value) => {
                value.to_py_object(py).as_object().into_py_object(py)
            }
            EntryValue::Str(value) => {
                value.to_py_object(py).as_object().into_py_object(py)
            }
            EntryValue::Bool(value) => {
                value.to_py_object(py).as_object().into_py_object(py)
            }
            EntryValue::ID(value) => {
                value.to_py_object(py).as_object().into_py_object(py)
            }
            EntryValue::Decimal(value) => {
                let converted_value = value.to_string();
                converted_value.to_py_object(py).as_object().into_py_object(py)
            }
        }
    }
}