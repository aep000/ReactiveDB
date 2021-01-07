use crate::{config::{config_reader::DbConfig}, database_manager::DatabaseManager, hooks::listener_hook::NewListenerObj};
use crate::networking::types::{
    DBRequest, DBResponse, Query, RequestResponse, ToClientMessage,
};
use crate::read_config_file;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;
use std::fs;
use std::io;

pub fn start_db_thread(
    mut request_reciever: Receiver<(DBRequest, Uuid)>,
    mut response_channel_reciever: Receiver<(Sender<ToClientMessage>, Uuid)>,
    config_file: String,
) -> std::io::Result<()> {
    let config:DbConfig = read_config_file(config_file.to_string())?;
    let destination = config.storage_destination.clone();
    match fs::create_dir(destination.clone()) {
        Ok(()) => {},
        Err(e) => {
            match e.kind() {
                io::ErrorKind::AlreadyExists => {},
                _ => panic!(e)
            }
        }
    };
    let mut dbm = DatabaseManager::from_config(config, destination).unwrap();

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
            response_channels.insert(new_client_id, new_channel);
        }
        let response_channel = response_channels.get(&client_id).unwrap();
        match request {
            DBRequest::Query(query_request) => {
                let query = query_request.query;
                let id = query_request.request_id;
                match query {
                    Query::FindOne(request) => {
                        let found_one = dbm.find_one(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::OneResult(found_one),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::LessThan(request) => {
                        let found_many =
                            dbm.less_than_search(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(found_many),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::GreaterThan(request) => {
                        let found_many =
                            dbm.greater_than_search(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(found_many),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::InsertData(request) => {
                        println!("Insert Request {:?}", request);
                        let (temp_dbm, results) = dbm.insert_entry(&request.table, request.entry, None,);
                        dbm = temp_dbm;
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(results.map(|edits|{
                                edits.iter().map(|edit|{edit.entry.clone()}).collect()
                            })),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                    Query::DeleteData(request) => {
                        let (temp_dbm, results) = dbm.delete_all(&request.table, request.column, request.key);
                        dbm = temp_dbm;
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(results.map(|edits|{
                                edits.iter().map(|edit|{edit.entry.clone()}).collect()
                            })),
                        });
                        let _ = response_channel.blocking_send(response);
                    },
                    Query::GetAll(request) => {
                        let results = dbm.get_all(&request.table, request.column, request.key);
                        let response = ToClientMessage::RequestResponse(RequestResponse {
                            request_id: id,
                            response: DBResponse::ManyResults(results),
                        });
                        let _ = response_channel.blocking_send(response);
                    }
                }
            }
            DBRequest::StartListen(listen_request) => {
                let new_listener = NewListenerObj {
                    uuid: client_id,
                    channel: response_channel.clone(),
                    event: listen_request.event
                };

                match dbm.add_listener(new_listener, &listen_request.table_name) {
                    Err(e) => panic!(e),
                    _ => {}
                };
            }
        };
    }
}
