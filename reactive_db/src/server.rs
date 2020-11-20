
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

struct ClientThread{
    id: Uuid,
    db_request_channel: Sender<(DBRequest, Uuid)>,
    db_result_channel: Receiver<DBResponse>
}
impl ClientThread {
    fn new(id: Uuid, db_request_channel: Sender<(DBRequest, Uuid)>, db_result_channel: Receiver<DBResponse>) -> ClientThread{
        return ClientThread {
            id: id,
            db_request_channel: db_request_channel,
            db_result_channel: db_result_channel
        };
    }
    fn start(&self, mut stream: TcpStream) -> std::io::Result<()> {
        let mut n = 0;
        loop {
            n+=1;
            print!("\rconnection id: {} | request count: {}", self.id, n);
            let mut size_buffer = [0; 4];
            stream.read(&mut size_buffer)?;
            let message_size = Cursor::new(size_buffer).read_u32::<BigEndian>().unwrap() as usize;
            if message_size == 0 {
                return Ok(());
            }
            let mut message_buffer = vec![0; message_size];
            stream.read(&mut message_buffer)?;
            let json_result: serde_json::Result<DBRequest> = serde_json::from_slice(message_buffer.as_slice());
            //println!("{:?}", size_buffer);
            // Request db thread for results
            let _ = match json_result {
                Ok(request) => self.db_request_channel.send((request, self.id)),
                Err(e) => panic!(format!("{:?}, {:?}", e, message_buffer))
            };

            let db_result = match self.db_result_channel.recv() {
                Ok(result) => result,
                Err(e) => DBResponse::NoResult(Err(format!("{:?}", e)))
            };
            let serialized_result = match serde_json::to_vec(&db_result) {
                Ok(r) => r,
                Err(e) => panic!(e)
            };
            stream.write_u32::<BigEndian>(serialized_result.len() as u32)?;
            stream.write(serialized_result.as_slice())?;
        }
    }
}

fn db_thread(request_reciever: Receiver<(DBRequest, Uuid)>, response_channel_reciever: Receiver<(Sender<DBResponse>, Uuid)>, config_file: String) -> std::io::Result<()>{
    let config = read_config_file(config_file.to_string())?;
    let db = Database::from_config(config, "db/".to_string());
    let mut db = match db {
        Ok(db) => db,
        Err(e) => panic!(e)
    };
    let mut response_channels: HashMap<Uuid, Sender<DBResponse>> = HashMap::new();

    for (request, client_id) in request_reciever.iter() {
        for (new_channel, new_client_id) in response_channel_reciever.try_iter() {
            response_channels.insert(new_client_id, new_channel);
        }
        let response_channel = response_channels.get(&client_id).unwrap();
        match request {
            DBRequest::FindOne(request) => {
                let found_one = db.find_one(&request.table, request.column, request.key);
                response_channel.send(DBResponse::OneResult(found_one)).unwrap();
            },
            DBRequest::LessThan(request) => {
                let found_many = db.less_than_search(&request.table, request.column, request.key);
                response_channel.send(DBResponse::ManyResults(found_many)).unwrap();
            },
            DBRequest::GreaterThan(request) => {
                let found_many = db.greater_than_search(&request.table, request.column, request.key);
                response_channel.send(DBResponse::ManyResults(found_many)).unwrap();
            },
            DBRequest::InsertData(request) => {
                let results = db.insert_entry(&request.table, request.entry, None);
                response_channel.send(DBResponse::NoResult(results)).unwrap();
            },
            DBRequest::DeleteData(request) => {
                let results = db.delete_all(&request.table, request.column, request.key);
                response_channel.send(DBResponse::ManyResults(results)).unwrap();
            },
        };
    }
    Ok(())
}

#[tokio::main]
pub async fn start_server(port: String, config_file: String) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;

    let (db_request_sender, db_request_reciever) = channel();

    let (db_response_channel_sender, db_response_channel_reciever) = channel();

    thread::spawn(move ||  { 
        match db_thread(db_request_reciever, db_response_channel_reciever, config_file) {
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
            let client_thread = ClientThread::new(client_id, thread_db_request_copy, db_result_reciever);
            client_thread.start(stream?)
        });
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub enum DBRequest {
    FindOne(GetData),
    LessThan(GetData),
    GreaterThan(GetData),
    InsertData(InsertData),
    DeleteData(DeleteData)
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
