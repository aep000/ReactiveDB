
use std::collections::HashMap;
use crate::Entry;
use crate::EntryValue;
use std::sync::mpsc::{Receiver, Sender, channel};
use crate::read_config_file;
use crate::Database;
use crate::io::Cursor;
use std::net::TcpStream;
use std::net::TcpListener;
use std::thread;
use std::io::{Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use crate::db_thread;
use crate::client_connection;

#[tokio::main]
pub async fn start_server(port: String, config_file: String) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;

    let (db_request_sender, db_request_reciever) = channel();

    let (db_response_channel_sender, db_response_channel_reciever) = channel();

    tokio::spawn(async move { 
        match db_thread::start_db_thread(db_request_reciever, db_response_channel_reciever, config_file){
            Ok(()) => panic!("Server closing!"),
            Err(e) => panic!("{:?}",e)
        };
    });
    
    // accept connections and process them serially
    for stream in listener.incoming() {
        let client_id = Uuid::new_v4();
        let thread_db_request_copy = db_request_sender.clone();
        let (db_result_sender, db_result_reciever) = channel();
        db_response_channel_sender.send((db_result_sender, client_id.clone())).unwrap();
        tokio::spawn(async move { 
            client_connection::start_client_thread(client_id, thread_db_request_copy, db_result_reciever, stream.unwrap());
        });
    }
    Ok(())
}
