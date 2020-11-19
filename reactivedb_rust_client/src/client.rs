use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use crate::types::DBResponse;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use crate::types::DBRequest;
use std::io;
use std::io::{Read, Write, Cursor};

pub struct Client {
    addr: String,
    connection: Option<TcpStream>
}

impl Client {
    pub fn new(addr: &str) -> Client{
        Client {
            addr: addr.to_string(),
            connection: None
        }
    }

    pub fn open_connection(&mut self) -> io::Result<()> {
        self.connection = Some(TcpStream::connect(&self.addr)?);
        Ok(())
    }

    pub fn close_connection(&mut self) -> io::Result<()> {
        self.connection = None;
        Ok(())
    }

    pub fn make_request(&mut self, request: DBRequest) -> io::Result<DBResponse>{
        let mut stream = match &self.connection {
            Some(s) => s,
            None => Err(Error::new(ErrorKind::Other, "Connection to server not open"))?
        };
        let serialized_request = serde_json::to_string(&request).unwrap();
        let mut total_request: Vec<u8> = vec![];
        total_request.write_u32::<BigEndian>(serialized_request.len() as u32)?;
        let mut bytes = serialized_request.into_bytes();
        total_request.append(&mut bytes);
        stream.write(total_request.as_slice())?;

        let mut size_buffer = [0; 4];
        stream.read(&mut size_buffer)?;
        let message_size = Cursor::new(size_buffer).read_u32::<BigEndian>().unwrap() as usize;
        let mut message_buffer = vec![0; message_size];
        stream.read(&mut message_buffer)?;
        let results: serde_json::Result<DBResponse> = serde_json::from_slice(message_buffer.as_slice());
        Ok(results.unwrap())
    }
}