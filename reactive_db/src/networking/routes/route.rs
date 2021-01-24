use hyper::{Body, Request, Response};

use tokio::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;
use async_trait::async_trait;

use crate::networking::network_types::{DBRequest, ToClientMessage};

#[async_trait]
pub trait Route: Send + RouteClone {
    fn matches(&self, url: String) -> bool;
    async fn get_result(&self, req: Request<Body>, id: Uuid, db_request_channel: &Sender<(DBRequest, Uuid)>, db_result_channel: &Receiver<ToClientMessage>,) -> Result<Response<Body>, hyper::Error>;
}

pub trait RouteClone {
    fn clone_box(&self) -> Box<dyn Route>;
}

impl<T> RouteClone for T where T: 'static + Route + Clone {
    fn clone_box(&self) -> Box<dyn Route> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Route> {
    fn clone(&self) -> Box<dyn Route> {
        self.clone_box()
    }
}
