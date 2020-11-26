use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub struct ListenRequest {
    pub table_name: String,
    pub event: ListenEvent,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub enum Query {
    FindOne(GetData),
    LessThan(GetData),
    GetAll(GetData),
    GreaterThan(GetData),
    InsertData(InsertData),
    DeleteData(DeleteData),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub struct QueryRequest {
    pub request_id: Uuid,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord)]
pub enum DBRequest {
    Query(QueryRequest),
    StartListen(ListenRequest),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord, Clone)]
pub enum ListenEvent {
    Insert,
    Delete,
}

impl DBRequest {
    pub fn new_insert(table: String, entry: Entry) -> (DBRequest, Uuid) {
        let query = Query::InsertData(InsertData { table, entry });
        let request_id = Uuid::new_v4();
        (
            DBRequest::Query(QueryRequest { request_id, query }),
            request_id,
        )
    }
    pub fn new_find_one(table: String, column: String, key: EntryValue) -> (DBRequest, Uuid) {
        let query = Query::FindOne(GetData { table, column, key });
        let request_id = Uuid::new_v4();
        (
            DBRequest::Query(QueryRequest { request_id, query }),
            request_id,
        )
    }
    pub fn new_greater_than(table: String, column: String, key: EntryValue) -> (DBRequest, Uuid) {
        let query = Query::GreaterThan(GetData { table, column, key });
        let request_id = Uuid::new_v4();
        (
            DBRequest::Query(QueryRequest { request_id, query }),
            request_id,
        )
    }
    pub fn new_less_than(table: String, column: String, key: EntryValue) -> (DBRequest, Uuid) {
        let query = Query::LessThan(GetData { table, column, key });
        let request_id = Uuid::new_v4();
        (
            DBRequest::Query(QueryRequest { request_id, query }),
            request_id,
        )
    }
    pub fn new_get_all(table: String, column: String, key: EntryValue) -> (DBRequest, Uuid) {
        let query = Query::GetAll(GetData { table, column, key });
        let request_id = Uuid::new_v4();
        (
            DBRequest::Query(QueryRequest { request_id, query }),
            request_id,
        )
    }
    pub fn new_delete(table: String, column: String, key: EntryValue) -> (DBRequest, Uuid) {
        let query = Query::DeleteData(DeleteData { table, column, key });
        let request_id = Uuid::new_v4();
        (
            DBRequest::Query(QueryRequest { request_id, query }),
            request_id,
        )
    }
    pub fn new_listen(table: String, listen_event: ListenEvent) -> DBRequest {
        let listen_request = ListenRequest {
            table_name: table,
            event: listen_event,
        };
        DBRequest::StartListen(listen_request)
    }
}

pub type ClientRequest = (DBRequest, Uuid);

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord, Clone)]
pub struct GetData {
    pub table: String,
    pub column: String,
    pub key: EntryValue,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord, Clone)]
pub struct InsertData {
    pub table: String,
    pub entry: Entry,
}

pub type DeleteData = GetData;

#[derive(Serialize, Deserialize, Debug, Eq, PartialOrd, PartialEq, Ord, Clone)]
pub enum DBResponse {
    ManyResults(Result<Vec<Entry>, String>),
    OneResult(Result<Option<Entry>, String>),
    NoResult(Result<(), String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialOrd, PartialEq, Ord)]
pub enum EntryValue {
    Integer(isize),
    Array(Vec<EntryValue>),
    Map(Entry),
    //Float(f64),
    Str(String),
    Bool(bool),
    ID(String),
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

#[derive(Serialize, Deserialize, Clone)]
pub enum ToClientMessage {
    Event(ListenResponse),
    RequestResponse(RequestResponse),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RequestResponse {
    pub request_id: Uuid,
    pub response: DBResponse,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ListenResponse {
    pub table_name: String,
    pub event: ListenEvent,
    pub value: DBResponse,
}
