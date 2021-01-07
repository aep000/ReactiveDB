use crate::networking::client_connection;
use crate::db_thread;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

#[tokio::main]
pub async fn start_server(port: String, config_file: String) -> std::io::Result<()> {
    let (db_request_sender, db_request_reciever) = channel(200);

    let (db_response_channel_sender, db_response_channel_reciever) = channel(200);

    let db_thread = thread::spawn(|| {
        match db_thread::start_db_thread(
            db_request_reciever,
            db_response_channel_reciever,
            config_file,
        ) {
            Ok(()) => panic!("Server closing!"),
            Err(e) => panic!("{:?}", e),
        };
    });

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
    db_thread.join().unwrap();

    Ok(())
}
