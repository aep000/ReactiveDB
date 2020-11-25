use crate::client_connection::ListenEvent;
use crate::client_connection::{DBResponse, ListenRequest, ListenResponse, ToClientMessage};
use crate::config::config_reader::{DbConfig, TableConfig};
use crate::config::parser::parse_transform_config;
use crate::constants::ROW_ID_COLUMN_NAME;
use crate::constants::SOURCE_ENTRY_ID;
use crate::transform::Transform;
use crate::types::Entry;
use crate::Column;
use crate::DataType;
use crate::EntryValue;
use crate::Table;
use crate::TableType;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

pub struct Database {
    pub tables: HashMap<String, Table>,
    listeners: HashMap<String, Vec<(ListenEvent, Uuid)>>,
    response_channels: HashMap<Uuid, Sender<ToClientMessage>>,
}

impl Database {
    pub fn from_config(config: DbConfig, storage_path: String) -> Result<Database, String> {
        let mut tables: HashMap<String, Table> = HashMap::new();
        for table in config.tables {
            match table {
                TableConfig::Source(source_config) => {
                    let name = source_config.name;
                    let mut columns = vec![];
                    for (name, data_type) in source_config.columns {
                        columns.push(Column::new(name, data_type))
                    }
                    columns.push(Column::new("_entryId".to_string(), DataType::ID));
                    let new_table = match Table::new(
                        name.clone(),
                        columns,
                        TableType::Source,
                        storage_path.clone(),
                    ) {
                        Ok(t) => Ok(t),
                        Err(e) => Err(format!("{:?}", e)),
                    }?;
                    tables.insert(name, new_table);
                }
                TableConfig::Derived(config) => {
                    let table = parse_transform_config(config, storage_path.clone())?;
                    tables.insert(table.name.clone(), table);
                }
            }
        }
        let mut input_refs = vec![];
        for (name, table) in &tables {
            for input_table_name in &table.input_tables {
                input_refs.push((input_table_name.clone(), name.clone()));
            }
        }
        for (source_table, dest_table) in input_refs {
            let table_to_mod = match tables.get_mut(&source_table) {
                Some(t) => t,
                None => Err("Specified input table does not exist".to_string())?,
            };
            table_to_mod.output_tables.push(dest_table.clone());
        }
        return Ok(Database {
            tables: tables,
            listeners: HashMap::new(),
            response_channels: HashMap::new(),
        });
    }

    pub fn find_one(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Option<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.find_one(column, &key) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {}", e)),
        }
    }

    pub fn delete_all(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };

        let mut to_delete: Vec<(String, EntryValue)> = vec![];
        let mut deleted = match table_obj.delete(column, &key) {
            Ok(deleted) => {
                for output_table in &table_obj.output_tables {
                    for entry in &deleted {
                        to_delete.push((
                            output_table.clone(),
                            entry.get(ROW_ID_COLUMN_NAME).unwrap().clone(),
                        ));
                        match self.listeners.get(table) {
                            Some(listener_list) => {
                                for (event, conn_id) in listener_list {
                                    match event {
                                        ListenEvent::Delete => {
                                            match self.response_channels.get(conn_id) {
                                                Some(channel) => {
                                                    let msg =
                                                        ToClientMessage::Event(ListenResponse {
                                                            table_name: table.to_string(),
                                                            event: ListenEvent::Delete,
                                                            value: DBResponse::OneResult(Ok(Some(
                                                                entry.clone(),
                                                            ))),
                                                        });
                                                    let _ = channel.blocking_send(msg);
                                                }
                                                None => {}
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            None => {}
                        };
                    }
                }
                deleted
            }
            Err(e) => Err(format!("Error when deleting for entries {}", e))?,
        };
        for (table, id) in to_delete {
            deleted.append(&mut self.delete_all(&table, SOURCE_ENTRY_ID.to_string(), id)?);
        }
        Ok(deleted)
    }

    // TODO clean this dumpster fire
    pub fn insert_entry<'a>(
        &mut self,
        table: &String,
        entry: Entry,
        source_table: Option<&String>,
    ) -> Result<(), String> {
        let output_tables = self.get_all_next_inserts(table);
        let transform = self.get_table_transform(table);
        let entry = match transform {
            Some(transform) => transform.execute(entry, table, self, source_table),
            None => Some(entry),
        };
        match self.listeners.get(table) {
            Some(listener_list) => {
                for (event, conn_id) in listener_list {
                    match event {
                        ListenEvent::Insert => match entry.clone() {
                            Some(entry_clone) => match self.response_channels.get(conn_id) {
                                Some(channel) => {
                                    let msg = ToClientMessage::Event(ListenResponse {
                                        table_name: table.to_string(),
                                        event: ListenEvent::Insert,
                                        value: DBResponse::OneResult(Ok(Some(entry_clone))),
                                    });
                                    let _ = channel.blocking_send(msg);
                                }
                                None => {}
                            },
                            None => {}
                        },
                        _ => {}
                    }
                }
            }
            None => {}
        };

        match self.tables.get_mut(table) {
            Some(t) => {
                match entry {
                    Some(unwrapped_entry) => match t.insert(unwrapped_entry) {
                        Ok(inserted_entry_results) => match inserted_entry_results {
                            Some(inserted_entry_unwrapped) => {
                                for output_table in output_tables {
                                    self.insert_entry(
                                        &output_table,
                                        inserted_entry_unwrapped.clone(),
                                        Some(table),
                                    )?;
                                }
                                ()
                            }
                            None => (),
                        },
                        Err(e) => Err(format!("Error when inserting entry {}", e))?,
                    },
                    None => (),
                };
            }
            None => Err(format!("Unable to find table {}", table))?,
        };
        return Ok(());
    }

    pub fn less_than_search(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.less_than(column, key, false) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {}", e)),
        }
    }

    pub fn greater_than_search(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.greater_than(column, key) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {:?}", e)),
        }
    }

    pub fn add_listener(&mut self, listen_request: ListenRequest, client_id: Uuid) {
        let mut listener_list = match self.listeners.remove(&listen_request.table_name) {
            Some(listener_list) => listener_list,
            None => vec![],
        };
        listener_list.push((listen_request.event, client_id));
        self.listeners
            .insert(listen_request.table_name, listener_list);
    }

    pub fn add_response_channel(
        &mut self,
        client_id: Uuid,
        response_channel: Sender<ToClientMessage>,
    ) {
        self.response_channels.insert(client_id, response_channel);
    }

    fn get_all_next_inserts(&self, table: &String) -> Vec<String> {
        match self.tables.get(table) {
            Some(t) => t.output_tables.clone(),
            None => vec![],
        }
    }

    fn get_table_transform(&self, table: &String) -> Option<Transform> {
        match self.tables.get(table) {
            Some(t) => match &t.table_type {
                TableType::Source => None,
                TableType::Derived(transform) => Some(transform.clone()),
            },
            None => None,
        }
    }
}
