

use crate::{actions::workspace::Workspace, database::db_trait::DB, types::CommitedEdit};
use crate::networking::types::{ListenResponse, DBResponse, ToClientMessage, ListenEvent};
use crate::hooks::hook::Event;
use crate::Entry;
use crate::types::DBEdit;
use crate::hooks::hook::Hook;
use futures::FutureExt;
use uuid::Uuid;
use std::collections::HashMap;
use tokio::sync::mpsc::{Sender, Receiver, channel};

pub struct ListenerHook {
    listeners: HashMap<ListenEvent, Vec<Uuid>>,
    new_listener_reciever: Receiver<NewListenerObj>,
    response_channels: HashMap<Uuid, Sender<ToClientMessage>>,
    table: String
}

impl ListenerHook {
    pub fn new(table: String) -> (Sender<NewListenerObj>, ListenerHook) {
        let (new_listener_sender, new_listener_reciever) = channel(1000);

        return (new_listener_sender, ListenerHook {
            listeners: HashMap::new(),
            new_listener_reciever,
            response_channels: HashMap::new(),
            table
        });
    }

    fn update_listeners(&mut self) {
        loop {
            // TODO investigate what it means for the inner and outer optionals are None
            let new_listener = match self.new_listener_reciever.recv().now_or_never() {
                Some(v) => match v {
                    Some(v) => v,
                    _ => break,
                },
                _ => break,
            };
            let event_listeners = match self.listeners.get_mut(&new_listener.event) {
                Some(connection_id) => connection_id,
                None => {
                    self.listeners.insert(new_listener.event.clone(), vec![]);
                    self.listeners.get_mut(&new_listener.event).unwrap()
                }
            };
            event_listeners.push(new_listener.uuid);
            self.response_channels.insert(new_listener.uuid, new_listener.channel);
        };
    }
}

impl Hook for ListenerHook {
    fn execute(&mut self, event: Event, proposed_edits:Option<Vec<DBEdit>>, commited_edits: Option<Vec<CommitedEdit>>, _db: &mut dyn DB, _workspace: Workspace) -> Option<Vec<DBEdit>>{
        let default = vec![];
        self.update_listeners();
        let (levent, listener_list, entries) = match event {
            Event::PostDelete => {
                let entries: Vec<Entry> = commited_edits.unwrap().iter().map(|edit:&CommitedEdit| -> Entry {edit.entry.clone()}).collect();
                let listener_list = self.listeners.get(&ListenEvent::Delete).unwrap_or(&default);
                (ListenEvent::Delete, listener_list, entries)
            }
            Event::PostInsert(_) => {
                let entries: Vec<Entry> = commited_edits.unwrap().iter().map(|edit:&CommitedEdit| -> Entry {edit.entry.clone()}).collect();
                let listener_list = self.listeners.get(&ListenEvent::Insert).unwrap_or(&default);
                (ListenEvent::Insert, listener_list, entries)
            }
            _ => {return proposed_edits}
        };

        for listener in listener_list {
            let listener_channel = self.response_channels.get(listener).unwrap();
            let msg = ToClientMessage::Event(ListenResponse {
                table_name: self.table.clone(),
                event: levent.clone(),
                value: DBResponse::ManyResults(Ok(entries.clone())),
            });
            listener_channel.blocking_send(msg);
        }
        return proposed_edits;
    }
    fn get_events(&self) -> Vec<Event>{return vec![Event::PostInsert(None), Event::PostDelete]}
    fn get_table(&self) -> String{self.table.clone()}
}

pub struct NewListenerObj {
    pub uuid: Uuid,  
    pub channel: Sender<ToClientMessage>,
    pub event: ListenEvent
}