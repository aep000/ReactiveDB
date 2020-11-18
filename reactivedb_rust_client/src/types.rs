use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub enum DBRequest {
    FindOne(GetData),
    LessThan(GetData),
    GreaterThan(GetData),
    InsertData(InsertData),
    DeleteData(DeleteData)
}

impl DBRequest {
    pub fn new_insert(table: String, entry: Entry) -> DBRequest{
        DBRequest::InsertData(InsertData {table, entry})
    }
    pub fn new_find_one(table: String, column: String, key: EntryValue) -> DBRequest{
        DBRequest::FindOne(GetData {table, column, key})
    }
    pub fn new_greater_than(table: String, column: String, key: EntryValue) -> DBRequest{
        DBRequest::GreaterThan(GetData {table, column, key})
    }
    pub fn new_less_than(table: String, column: String, key: EntryValue) -> DBRequest{
        DBRequest::LessThan(GetData {table, column, key})
    }
    pub fn new_delete(table: String, column: String, key: EntryValue) -> DBRequest{
        DBRequest::DeleteData(DeleteData {table, column, key})
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub struct GetData {
    pub table: String,
    pub column: String,
    pub key: EntryValue
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub struct InsertData {
    pub table: String,
    pub entry: Entry,
}

pub type DeleteData = GetData;

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub enum DBResponse {
    ManyResults(Result<Vec<Entry>, String>),
    OneResult(Result<Option<Entry>, String>),
    NoResult(Result<(), String>)
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

pub type Entry = BTreeMap<String, EntryValue>;

#[derive(Clone)]
pub struct EntryBuilder {
    map: Entry,
}

impl EntryBuilder {
    pub fn new() -> EntryBuilder {
        return EntryBuilder {
            map: BTreeMap::new(),
        };
    }
    pub fn column(&mut self, key: &str, value: EntryValue) -> EntryBuilder {
        self.map.insert(key.to_string(), value);
        return self.clone();
    }
    pub fn build(&mut self) -> Entry {
        self.map.clone()
    }
}