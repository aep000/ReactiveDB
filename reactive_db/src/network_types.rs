use serde::{Serialize, Deserialize};
use uuid::Uuid;
use crate::types::{Entry, EntryValue};

// ===============Response types================
#[derive(Serialize, Deserialize)]
pub enum ToClientMessage {
    Event(ListenResponse),
    RequestResponse(RequestResponse),
}

#[derive(Serialize, Deserialize)]
pub struct ListenResponse {
    pub table_name: String,
    pub event: ListenEvent,
    pub value: DBResponse,
}

#[derive(Serialize, Deserialize)]
pub struct RequestResponse {
    pub request_id: Uuid,
    pub response: DBResponse,
}

#[derive(Serialize, Deserialize)]
pub enum DBResponse {
    ManyResults(Result<Vec<Entry>, String>),
    OneResult(Result<Option<Entry>, String>),
    NoResult(Result<(), String>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ListenEvent {
    Insert,
    Delete,
}

// ======================Request Types=========================

#[derive(Serialize, Deserialize)]
pub enum DBRequest {
    Query(QueryRequest),
    StartListen(ListenRequest),
}

#[derive(Serialize, Deserialize)]
pub struct QueryRequest {
    pub request_id: Uuid,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListenRequest {
    pub table_name: String,
    pub event: ListenEvent,
}

#[derive(Serialize, Deserialize)]
pub enum Query {
    FindOne(GetData),
    LessThan(GetData),
    GetAll(GetData),
    GreaterThan(GetData),
    InsertData(InsertData),
    DeleteData(DeleteData),
}

#[derive(Serialize, Deserialize)]
pub struct GetData {
    pub table: String,
    pub column: String,
    pub key: EntryValue,
}

#[derive(Serialize, Deserialize)]
pub struct InsertData {
    pub table: String,
    pub entry: Entry,
}

pub type DeleteData = GetData;
