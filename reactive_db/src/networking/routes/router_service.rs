use std::{pin::Pin, task::{Context, Poll}};

use futures::Future;
use hyper::{Body, Request, Response, StatusCode, service::Service};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use uuid::Uuid;

use crate::networking::network_types::{DBRequest, ToClientMessage};

use super::Route;

pub struct RouterService {
    routes: Vec<Box<dyn Route>>,
    id: Uuid,
    db_request_channel: Sender<(DBRequest, Uuid)>,
    db_result_channel: Receiver<ToClientMessage>,
}

impl Service<Request<Body>> for RouterService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        for route in self.routes.clone() {

            if route.matches(req.uri().path().to_string()){
                //let result =  route.clone().get_result(req, self.id, self.db_request_channel, &self.db_result_channel);
                //return Box::pin(result);
            }
        }
        let body: Body = Body::from("404 Route Not Found");
        let resp = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(body)
            .expect("Unable to create `Response`");
        Box::pin(async {Ok(resp)})
    }
}

pub struct MakeRouterService {
    pub routes: Vec<Box<dyn Route>>,
    pub db_request_channel: Sender<(DBRequest, Uuid)>,
    pub db_result_channel_sender: Sender<(Sender<ToClientMessage>, Uuid)> 
}

impl<T> Service<T> for MakeRouterService {
    type Response = RouterService;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: T) -> Self::Future {
        let routes= self.routes.clone();//.iter().map(|route| {route.clone_box()}).collect();
        let id = Uuid::new_v4(); 
        let db_request_channel = self.db_request_channel.clone();
        let (db_result_sender, db_result_reciever) = channel(30);
        self.db_result_channel_sender.send((db_result_sender, id));

        let fut = async move { Ok(RouterService { routes, id, db_request_channel, db_result_channel: db_result_reciever }) };
        Box::pin(fut)
    }
}