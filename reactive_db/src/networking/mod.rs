mod network_types;
mod routes;

pub mod client_connection;

pub mod types {

    pub use super::network_types::{
        ToClientMessage,
        ListenResponse,
        RequestResponse,
        DBResponse,
        ListenEvent,
        DBRequest,
        QueryRequest,
        ListenRequest,
        Query,
        GetData,
        DeleteData,
        InsertData,
    };
}

pub mod web_thread;