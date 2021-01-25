use std::net::SocketAddr;

use hyper::Server;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use super::{network_types::{DBRequest, ToClientMessage}, routes::{MakeRouterService, Route}};

// Concept: Standup a web interface for users to hit both actions and auto generated apis
// Needs: Hold a reference to the db request messenger, keep track of responses with unique response ids.
// Treat like a regular client thread? Have one response channel and divy on that? Or open a new channel for each request?
pub async fn web_thread(
    routes: Vec<Box<dyn Route>>,
    db_request_channel: Sender<(DBRequest, Uuid)>,
    db_result_channel_sender: Sender<(Sender<ToClientMessage>, Uuid)> 
) {

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let server = Server::bind(&addr).serve(MakeRouterService {routes, db_request_channel, db_result_channel_sender});

    // Run this server for... forever!
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}