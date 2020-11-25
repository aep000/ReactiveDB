use crate::client_connection::{
    DBRequest, DBResponse, Query, RequestResponse, ToClientMessage,
};
use crate::read_config_file;
use crate::Database;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;

pub fn start_db_thread(
    mut request_reciever: Receiver<(DBRequest, Uuid)>,
    mut response_channel_reciever: Receiver<(Sender<ToClientMessage>, Uuid)>,
    config_file: String,
) -> std::io::Result<()> {
    let config = read_config_file(config_file.to_string())?;
    let db = Database::from_config(config, "db/".to_string());
    let mut db = match db {
        Ok(db) => db,
        Err(e) => panic!(e),
    };
    let mut response_channels: HashMap<Uuid, Sender<ToClientMessage>> = HashMap::new();

    loop {
        let (request, client_id) = match request_reciever.blocking_recv() {
            Some(v) => v,
            _ => continue,
        };
        loop {
            let (new_channel, new_client_id) = match response_channel_reciever.try_recv() {
                Ok(v) => v,
                _ => break,
            };
            let channel_clone = new_channel.clone();
            response_channels.insert(new_client_id, new_channel);
            db.add_response_channel(new_client_id, channel_clone);
        }
        let response_channel = response_channels.get(&client_id).unwrap();
        match request {
            DBRequest::Query(query_request) => {
                let query = query_request.query;
                let id = query_request.request_id;
                match query {
                    Query::FindOne(request) => {
                        let found_one = db.find_one(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::OneResult(found_one),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::LessThan(request) => {
                        let found_many =
                            db.less_than_search(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(found_many),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::GreaterThan(request) => {
                        let found_many =
                            db.greater_than_search(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(found_many),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::InsertData(request) => {
                        let results = db.insert_entry(&request.table, request.entry, None);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::NoResult(results),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::DeleteData(request) => {
                        let results = db.delete_all(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(results),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                }
            }
            DBRequest::StartListen(listen_request) => {
                db.add_listener(listen_request, client_id);
            }
        };
    }
}
