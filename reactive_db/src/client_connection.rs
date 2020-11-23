use crate::EntryValue;
use crate::Entry;
use uuid::Uuid;
use std::net::{TcpStream};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::io::Cursor;
use std::io::{Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Serialize, Deserialize};

pub fn start_client_thread(id: Uuid,
    db_request_channel: Sender<(DBRequest, Uuid)>,
    db_result_channel: Receiver<ToClientMessage>,
    stream: TcpStream){
        let stream_clone = stream.try_clone().expect("clone failed...");
        tokio::spawn(async move {
            handle_results(stream_clone, db_result_channel);
        });
        tokio::spawn(async move {
            handle_incoming_messages(stream, id, db_request_channel).unwrap();
        });
}

fn handle_results(mut stream: TcpStream, db_result_channel: Receiver<ToClientMessage>){
    loop {
        let db_result = db_result_channel.recv().unwrap();
        let serialized_result = match serde_json::to_vec(&db_result) {
            Ok(r) => r,
            Err(e) => panic!(e)
        };
        stream.write_u32::<BigEndian>(serialized_result.len() as u32).unwrap();
        stream.write(serialized_result.as_slice()).unwrap();
    }
}

fn handle_incoming_messages(mut stream: TcpStream, id: Uuid, db_request_channel: Sender<(DBRequest, Uuid)>) -> std::io::Result<()> {
    let mut n = 0;
    loop {
        n+=1;
        print!("\rconnection id: {} | request count: {}", id, n);
        let mut size_buffer = [0; 4];
        stream.read(&mut size_buffer)?;
        let message_size = Cursor::new(size_buffer).read_u32::<BigEndian>().unwrap() as usize;
        if message_size == 0 {
            return Ok(());
        }
        let mut message_buffer = vec![0; message_size];
        stream.read(&mut message_buffer)?;
        let json_response: serde_json::Result<DBRequest> = serde_json::from_slice(message_buffer.as_slice());
        //println!("{:?}", size_buffer);
        // Request db thread for results
        let _ = match json_response {
            Ok(request) => db_request_channel.send((request, id)),
            Err(e) => panic!(format!("{:?}, {:?}", e, message_buffer))
        };
    }
}

#[derive(Serialize, Deserialize)]
pub struct ListenResponse {
    pub table_name: String,
    pub event: ListenEvent,
    pub value: DBResponse,
}

#[derive(Serialize, Deserialize)]
pub struct RequestResponse{
    pub request_id: Uuid,
    pub response: DBResponse
}

#[derive(Serialize, Deserialize)]
pub enum ListenEvent {
    Insert,
    Delete
}

#[derive(Serialize, Deserialize)]
pub enum ToClientMessage {
    Event(ListenResponse),
    RequestResponse(RequestResponse)
}

#[derive(Serialize, Deserialize)]
pub struct ListenRequest {
    pub table_name: String,
    pub event: ListenEvent,
}

#[derive(Serialize, Deserialize)]
pub enum Query {
    FindOne(GetData),
    LessThan(GetData),
    GreaterThan(GetData),
    InsertData(InsertData),
    DeleteData(DeleteData)
}

#[derive(Serialize, Deserialize)]
pub struct QueryRequest {
    pub request_id: Uuid,
    pub query: Query
}

#[derive(Serialize, Deserialize)]
pub enum DBRequest {
    Query(QueryRequest),
    StartListen(ListenRequest)
}

#[derive(Serialize, Deserialize)]
pub struct GetData {
    pub table: String,
    pub column: String,
    pub key: EntryValue
}

#[derive(Serialize, Deserialize)]
pub struct InsertData {
    pub table: String,
    pub entry: Entry,
}

pub type DeleteData = GetData;

#[derive(Serialize, Deserialize)]
pub enum DBResponse {
    ManyResults(Result<Vec<Entry>, String>),
    OneResult(Result<Option<Entry>, String>),
    NoResult(Result<(), String>)
}