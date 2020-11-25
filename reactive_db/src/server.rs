use crate::client_connection;
use crate::db_thread;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

#[tokio::main]
pub async fn start_server(port: String, config_file: String) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    let (db_request_sender, db_request_reciever) = channel(200);

    let (db_response_channel_sender, db_response_channel_reciever) = channel(200);

    thread::spawn(|| {
        match db_thread::start_db_thread(
            db_request_reciever,
            db_response_channel_reciever,
            config_file,
        ) {
            Ok(()) => panic!("Server closing!"),
            Err(e) => panic!("{:?}", e),
        };
    });

    // accept connections and process them serially
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let client_id = Uuid::new_v4();
        let thread_db_request_copy = db_request_sender.clone();
        let (db_result_sender, db_result_reciever) = channel(30);
        db_response_channel_sender
            .send((db_result_sender, client_id.clone()))
            .await;
        tokio::spawn(async move {
            client_connection::start_client_thread(
                client_id,
                thread_db_request_copy,
                db_result_reciever,
                stream,
            );
        });
    }
    Ok(())
}
