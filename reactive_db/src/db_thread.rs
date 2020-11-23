use std::collections::HashMap;
use crate::Database;
use std::sync::mpsc::{Receiver, Sender};
use crate::read_config_file;
use crate::client_connection::{DBRequest, QueryRequest, DBResponse, ToClientMessage, RequestResponse, Query};
use uuid::Uuid;


pub fn start_db_thread(request_reciever: Receiver<(DBRequest, Uuid)>, response_channel_reciever: Receiver<(Sender<ToClientMessage>, Uuid)>, config_file: String) -> std::io::Result<()>{
    let config = read_config_file(config_file.to_string())?;
    let db = Database::from_config(config, "db/".to_string());
    let mut db = match db {
        Ok(db) => db,
        Err(e) => panic!(e)
    };
    let mut response_channels: HashMap<Uuid, Sender<ToClientMessage>> = HashMap::new();

    for (request, client_id) in request_reciever.iter() {
        for (new_channel, new_client_id) in response_channel_reciever.try_iter() {
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
                            response: DBResponse::OneResult(found_one)
                        });
                        response_channel.send(response).unwrap();
                    },
                    Query::LessThan(request) => {
                        let found_many = db.less_than_search(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(found_many)
                        });
                        response_channel.send(response).unwrap();
                    },
                    Query::GreaterThan(request) => {
                        let found_many = db.greater_than_search(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(found_many)
                        });
                        response_channel.send(response).unwrap();
                    },
                    Query::InsertData(request) => {
                        let results = db.insert_entry(&request.table, request.entry, None);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::NoResult(results)
                        });
                        response_channel.send(response).unwrap();
                    },
                    Query::DeleteData(request) => {
                        let results = db.delete_all(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(results)
                        });
                        response_channel.send(response).unwrap();
                    },
                }
            },
            DBRequest::StartListen(listen_request) => {
                db.add_listener(listen_request, client_id);
            }
        };
    }
    Ok(())
}