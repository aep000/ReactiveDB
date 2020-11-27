use crate::types::ClientRequest;
use crate::types::DBRequest;
use crate::types::ListenEvent;
use crate::types::{DBResponse, ToClientMessage};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::Cursor;
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct Client {
    addr: String,
    connection: Option<WriteHalf<TcpStream>>,
    response_subscribe_channel: Option<SubscriptionMaker<ToClientMessage>>,
}

impl Client {
    pub fn new(addr: &str) -> Client {
        Client {
            addr: addr.to_string(),
            connection: None,
            response_subscribe_channel: None,
        }
    }

    pub async fn open_connection(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(&self.addr).await?;
        let (read, write) = tokio::io::split(stream);
        self.connection = Some(write);
        let (subscription_channel, subscription_manager) =
            SubscriptionManager::<ToClientMessage>::new();
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

    pub async fn make_request(&mut self, client_request: ClientRequest) -> io::Result<DBResponse> {
        let (request, request_id) = client_request;
        match &mut self.connection {
            Some(stream) => {
                let serialized_request = serde_json::to_string(&request).unwrap();
                let mut total_request: Vec<u8> = vec![];
                WriteBytesExt::write_u32::<BigEndian>(
                    &mut total_request,
                    serialized_request.len() as u32,
                )?;
                let mut bytes = serialized_request.into_bytes();
                total_request.append(&mut bytes);
                stream.write(total_request.as_slice()).await?;
            }
            None => Err(Error::new(
                ErrorKind::Other,
                "Connection to server not open",
            ))?,
        };
        let mut receiver = match &mut self.response_subscribe_channel {
            Some(subscription_maker) => subscription_maker.subscribe(Box::new(move |entry: ToClientMessage| -> bool{
                match entry {
                    ToClientMessage::RequestResponse(message) => message.request_id == request_id.clone(),
                    _ => false
                }
            })).await,
            None => Err(Error::new(
                ErrorKind::Other,
                "Connection to server not open",
            ))?,
        };
        loop {
            match receiver.recv().await.unwrap() {
                ToClientMessage::RequestResponse(response) => {
                    if response.request_id == request_id {
                        return Ok(response.response);
                    }
                }
                ToClientMessage::Event(_) => {}
            }
        }
    }

    pub async fn subscribe_to_event(
        &mut self,
        table_name: String,
        event: ListenEvent,
        callback: Box<dyn Fn(DBResponse) -> Result<(), ()> + Send>,
    ) -> io::Result<()> {
        let request = DBRequest::new_listen(table_name.clone(), event.clone());
        match &mut self.connection {
            Some(stream) => {
                let serialized_request = serde_json::to_string(&request).unwrap();
                let mut total_request: Vec<u8> = vec![];
                WriteBytesExt::write_u32::<BigEndian>(
                    &mut total_request,
                    serialized_request.len() as u32,
                )?;
                let mut bytes = serialized_request.into_bytes();
                total_request.append(&mut bytes);
                stream.write(total_request.as_slice()).await?;
            }
            None => Err(Error::new(
                ErrorKind::Other,
                "Connection to server not open",
            ))?,
        };

        let mut receiver = match &mut self.response_subscribe_channel {
            Some(subscription_maker) => subscription_maker.subscribe(Box::new(move |entry: ToClientMessage| -> bool{
                match entry {
                    ToClientMessage::Event(message) => message.event == event.clone() && message.table_name == table_name.clone(),
                    _ => false
                }
            })).await,
            None => Err(Error::new(
                ErrorKind::Other,
                "Connection to server not open",
            ))?,
        };
        tokio::spawn(async move {
            loop {
                match receiver.recv().await.unwrap() {
                    ToClientMessage::RequestResponse(_) => {}
                    ToClientMessage::Event(event) => {
                        let _ = callback(event.value);
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn subscribe_to_event_blocking(
        &mut self,
        table_name: String,
        event: ListenEvent,
        callback: Box<dyn Fn(DBResponse) -> Result<(), ()> + Send>,
    ) -> io::Result<()> {
        let request = DBRequest::new_listen(table_name.clone(), event.clone());
        match &mut self.connection {
            Some(stream) => {
                let serialized_request = serde_json::to_string(&request).unwrap();
                let mut total_request: Vec<u8> = vec![];
                WriteBytesExt::write_u32::<BigEndian>(
                    &mut total_request,
                    serialized_request.len() as u32,
                )?;
                let mut bytes = serialized_request.into_bytes();
                total_request.append(&mut bytes);
                stream.write(total_request.as_slice()).await?;
            }
            None => Err(Error::new(
                ErrorKind::Other,
                "Connection to server not open",
            ))?,
        };

        let mut receiver = match &mut self.response_subscribe_channel {
            Some(subscription_maker) => subscription_maker.subscribe(Box::new(move |entry: ToClientMessage| -> bool{
                match entry {
                    ToClientMessage::Event(message) => message.event == event.clone() && message.table_name == table_name.clone(),
                    _ => false
                }
            })).await,
            None => Err(Error::new(
                ErrorKind::Other,
                "Connection to server not open",
            ))?,
        };
        loop {
            match receiver.recv().await.unwrap() {
                ToClientMessage::RequestResponse(_) => {}
                ToClientMessage::Event(event) => {
                    let _ = callback(event.value);
                }
            }
        }
    }

    async fn listen_for_messages(
        mut stream: ReadHalf<TcpStream>,
        mut subscription_manager: SubscriptionManager<ToClientMessage>,
    ) {
        loop {
            let mut size_buffer = [0; 4];
            stream.read(&mut size_buffer).await.unwrap();
            let message_size = ReadBytesExt::read_u32::<BigEndian>(&mut Cursor::new(size_buffer))
                .unwrap() as usize;
            let mut message_buffer = vec![0; message_size];
            stream.read(&mut message_buffer).await.unwrap();
            let results: serde_json::Result<ToClientMessage> =
                serde_json::from_slice(message_buffer.as_slice());
            let db_response = results.unwrap();
            subscription_manager.send_message(db_response).await;
        }
    }
}

type ClosureType<T> = Box<dyn Fn(T)->bool + Send + Sync>;

struct SubscriptionMaker<T> {
    add_sub_channel: Sender<(Sender<T>, ClosureType<T>)>,
}

impl<T> SubscriptionMaker<T> {
    fn new(add_sub_channel: Sender<(Sender<T>, ClosureType<T>)>) -> SubscriptionMaker<T> {
        SubscriptionMaker { add_sub_channel }
    }
    async fn subscribe(&mut self, condition: ClosureType<T>) -> Receiver<T> {
        let (sub_sender, sub_reciever) = channel(30);
        match self.add_sub_channel.send((sub_sender, condition)).await {
            Ok(()) => {}
            Err(_) => panic!("Channel Dropped"),
        };
        return sub_reciever;
    }
}

struct SubscriptionManager<T> {
    add_sub_channel: Receiver<(Sender<T>, ClosureType<T>)>,
    subscriptions: Vec<(Sender<T>, ClosureType<T>)>,
}

impl<T: Clone> SubscriptionManager<T> {
    fn new() -> (SubscriptionMaker<T>, SubscriptionManager<T>) {
        let (subscribe_channel_sender, subscribe_channel_reciever) = channel(30);
        let sub = SubscriptionManager {
            add_sub_channel: subscribe_channel_reciever,
            subscriptions: vec![],
        };
        (SubscriptionMaker::new(subscribe_channel_sender), sub)
    }
    async fn send_message(&mut self, value: T) {
        loop {
            match self.add_sub_channel.try_recv() {
                Ok(new_sub) => self.subscriptions.push(new_sub),
                Err(_) => break,
            }
        }
        for (channel, condition) in &self.subscriptions {
            if condition(value.clone()){
                channel.send(value.clone()).await;
            }
        }
    }
}
