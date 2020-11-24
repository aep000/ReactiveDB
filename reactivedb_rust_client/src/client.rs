use crate::types::ListenEvent;
use crate::types::Entry;
use crate::types::ListenRequest;
use std::future::Future;
use tokio::io::{WriteHalf, ReadHalf};
use crate::types::ClientRequest;
use uuid::Uuid;
use std::io::{Error, ErrorKind};
use tokio::net::TcpStream;
use crate::types::{DBResponse,ToClientMessage};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use crate::types::DBRequest;
use std::io;
use std::io::{Cursor};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::io::{AsyncWriteExt, AsyncReadExt};

pub struct Client {
    addr: String,
    connection: Option<WriteHalf<TcpStream>>,
    response_subscribe_channel: Option<SubscriptionMaker<ToClientMessage>>
}

impl Client {
    pub fn new(addr: &str) -> Client{
        Client {
            addr: addr.to_string(),
            connection: None,
            response_subscribe_channel: None
        }
    }

    pub async fn open_connection(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(&self.addr).await?;
        let (read, write) = tokio::io::split(stream);
        self.connection = Some(write);
        let (subscription_channel, subscription_manager) = SubscriptionManager::<ToClientMessage>::new();
        tokio::spawn(async move {
            Client::listen_for_messages(read, subscription_manager).await;
        });
        self.response_subscribe_channel = Some(subscription_channel);
        Ok(())
    }

    pub fn close_connection(&mut self) -> io::Result<()> {
        self.connection = None;
        Ok(())
    }


    pub async fn make_request(&mut self, client_request: ClientRequest) -> io::Result<DBResponse>{
        let (request, request_id) = client_request;
        match &mut self.connection {
            Some(stream) => {
                let serialized_request = serde_json::to_string(&request).unwrap();
                let mut total_request: Vec<u8> = vec![];
                WriteBytesExt::write_u32::<BigEndian>(&mut total_request, serialized_request.len() as u32)?;
                let mut bytes = serialized_request.into_bytes();
                total_request.append(&mut bytes);
                stream.write(total_request.as_slice()).await?;
            },
            None => Err(Error::new(ErrorKind::Other, "Connection to server not open"))?
        };
        let mut receiver = match &mut self.response_subscribe_channel {
            Some(subscription_maker)=> {
                subscription_maker.subscribe().await
            }
            None => Err(Error::new(ErrorKind::Other, "Connection to server not open"))?
        };
        loop {
            match receiver.recv().await.unwrap() {
                ToClientMessage::RequestResponse(response) => {
                    if response.request_id == request_id{
                        return Ok(response.response);
                    }
                },
                ToClientMessage::Event(_) => {}
            }
        }

    }

    pub async fn subscribe_to_event(&mut self, table_name: String, event: ListenEvent, callback: Box<dyn Fn(DBResponse) -> Result<(), ()> + Send>)-> io::Result<()>{
        let request = DBRequest::new_listen(table_name, event);
        match &mut self.connection {
            Some(stream) => {
                let serialized_request = serde_json::to_string(&request).unwrap();
                let mut total_request: Vec<u8> = vec![];
                WriteBytesExt::write_u32::<BigEndian>(&mut total_request, serialized_request.len() as u32)?;
                let mut bytes = serialized_request.into_bytes();
                total_request.append(&mut bytes);
                stream.write(total_request.as_slice()).await?;
            },
            None => Err(Error::new(ErrorKind::Other, "Connection to server not open"))?
        };

        let mut receiver = match &mut self.response_subscribe_channel {
            Some(subscription_maker)=> {
                subscription_maker.subscribe().await
            }
            None => Err(Error::new(ErrorKind::Other, "Connection to server not open"))?
        };
        tokio::spawn(async move {
            loop {
                match receiver.recv().await.unwrap() {
                    ToClientMessage::RequestResponse(_) => {},
                    ToClientMessage::Event(event) => {
                        callback(event.value);
                    }
                }
            }
        });

        Ok(())
    }

    async fn listen_for_messages(mut stream: ReadHalf<TcpStream>, mut subscription_manager: SubscriptionManager<ToClientMessage>){
        loop {
            let mut size_buffer = [0; 4];
            stream.read(&mut size_buffer).await.unwrap();
            let message_size = ReadBytesExt::read_u32::<BigEndian>(&mut Cursor::new(size_buffer)).unwrap() as usize;
            let mut message_buffer = vec![0; message_size];
            stream.read(&mut message_buffer).await.unwrap();
            let results: serde_json::Result<ToClientMessage> = serde_json::from_slice(message_buffer.as_slice());
            let db_response = results.unwrap();
            subscription_manager.send_message(db_response).await;
        }
    }
}

struct SubscriptionMaker<T> {
    add_sub_channel: Sender<Sender<T>>
}

impl <T> SubscriptionMaker<T> {
    fn new(add_sub_channel: Sender<Sender<T>>) -> SubscriptionMaker<T> {
        SubscriptionMaker {
            add_sub_channel
        }
    }
    async fn subscribe(&mut self) -> Receiver<T> {
        let (sub_sender, sub_reciever) = channel(30);
        match self.add_sub_channel.send(sub_sender).await {
            Ok(()) => {},
            Err(e) => panic!("Channel Dropped")
        };
        return sub_reciever;
    }
}

struct SubscriptionManager<T> {
    add_sub_channel: Receiver<Sender<T>>,
    subscriptions: Vec<Sender<T>>
}

impl<T: Clone> SubscriptionManager<T> {
    fn new()-> (SubscriptionMaker<T>, SubscriptionManager<T>){
        let (subscribe_channel_sender, subscribe_channel_reciever) = channel(30);
        let sub = SubscriptionManager {
            add_sub_channel: subscribe_channel_reciever,
            subscriptions: vec![]
        };
        (SubscriptionMaker::new(subscribe_channel_sender), sub)
    }
    async fn send_message(&mut self, value:T){
        loop {
            match self.add_sub_channel.try_recv(){
                Ok(new_sub) => self.subscriptions.push(new_sub),
                Err(_) => break
            }
        }
        for channel in &self.subscriptions {
            channel.send(value.clone()).await;
        }
    }
}