use crate::{config::config_reader::{DbConfig, read_config_file}, networking::{client_connection}};
use crate::database::db_thread;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

#[tokio::main]
pub async fn start_server(port: String, config_file: String) -> std::io::Result<()> {
    let (db_request_sender, db_request_reciever) = channel(200);

    let (db_response_channel_sender, db_response_channel_reciever) = channel(200);
    let config:DbConfig = read_config_file(config_file.to_string())?;

    let db_thread = thread::spawn(|| {
        match db_thread::start_db_thread(
            db_request_reciever,
            db_response_channel_reciever,
            config,
        ) {
            Ok(()) => panic!("Server closing!"),
            Err(e) => panic!("{:?}", e),
        };
    });
    let _db_request_clone = db_request_sender.clone();
    let _db_response_channel_sender_clone = db_response_channel_sender.clone();

    tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let client_id = Uuid::new_v4();
            let thread_db_request_copy = db_request_sender.clone();
            let (db_result_sender, db_result_reciever) = channel(30);
            match db_response_channel_sender
                .send((db_result_sender, client_id.clone()))
                .await {
                    Ok(_) => {},
                    Err(_) => {continue}
                };
            tokio::spawn(async move {
                client_connection::start_client_thread(
                    client_id,
                    thread_db_request_copy,
                    db_result_reciever,
                    stream,
                );
            });
        }
    });
    // TODO Fix web server
    //let routes = vec![];

    //tokio::spawn(web_thread(routes, db_request_clone, db_response_channel_sender_clone));


    db_thread.join().unwrap();

    Ok(())
}
