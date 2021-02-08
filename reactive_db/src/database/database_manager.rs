use std::collections::HashMap;

use tokio::sync::mpsc::Sender;

use crate::{actions::{Action, workspace::Workspace}, config::{config_parser::parse_transform_config, config_reader::{DbConfig, Importable, TableConfig}}, table::{multi_source_table::MultiSourceTable, table_trait::Table, types::{Column, TableType}}};
use super::{db_trait::DB, database::Database};
use crate::hooks::{hook::Hook, listener_hook::{ListenerHook, NewListenerObj}, transforms::TransformHook};
use crate::table::storage_manager_table::StorageManagerTable;
use crate::types::{CommitedEdit, DataType, Entry, EntryValue};
use crate::hooks::transforms::Transform;


pub struct DatabaseManager{
    db: Database,
    hooks: HashMap<String, Vec<Box<dyn Hook>>>,
    add_listener_senders: HashMap<String, Sender<NewListenerObj>>, // Should this be abstracted?
    workspace: Workspace
}

impl DatabaseManager {
    pub fn from_config(config: DbConfig, storage_path: String) -> Result<DatabaseManager, String> {
        let mut tables: HashMap<String, MultiSourceTable> = HashMap::new();
        let mut hooks: HashMap<String, Vec<Box<dyn Hook>>> = HashMap::new();
        let mut add_listener_senders = HashMap::new();
        let mut actions: HashMap<String, Action> = HashMap::new();

        let mut workspace = Workspace::new("./".to_string());
        match config.action_config {
            Some(actions_cfg) => {
                match actions_cfg {
                    Importable::Import(_) => {panic!("Action config not processed correctly!")}
                    Importable::Inline(unwrapped_action_cfg) => {
                        workspace = Workspace::new(unwrapped_action_cfg.workspace_dir);
                        for action in unwrapped_action_cfg.actions {
                            let action_obj = Action::new(action.file, action.function);
                            actions.insert(action.name, action_obj);
                        }
                    }
                }
            }
            None => {}
        }

        for table in config.tables {
            match table {
                TableConfig::Source(source_config) => {
                    let name = source_config.name;
                    let mut columns = vec![];
                    for (name, data_type) in source_config.columns {
                        columns.push(Column::new(name, data_type))
                    }
                    columns.push(Column::new("_entryId".to_string(), DataType::ID));
                    let new_table = match StorageManagerTable::new(
                        name.clone(),
                        columns,
                        TableType::Source,
                        storage_path.clone(),
                    ) {
                        Ok(t) => Ok(t),
                        Err(e) => Err(format!("{:?}", e)),
                    }?;
                    let mut hook_list:Vec<Box<dyn Hook>> = vec![Box::new(TransformHook::new(Transform::None, name.clone()))];
                    let (sender, listener_hook) = ListenerHook::new(name.clone());
                    add_listener_senders.insert(name.clone(), sender);
                    hook_list.push(Box::new(listener_hook));
                    hooks.insert(name.clone(), hook_list);
                    tables.insert(name, MultiSourceTable::InHouse(new_table));
                }
                TableConfig::Derived(config) => {
                    let (table, transform) = parse_transform_config(config, storage_path.clone(), &actions)?;
                    let table_name = table.name.clone();

                    let mut hook_list: Vec<Box<dyn Hook>> = vec![Box::new(TransformHook::new(transform, table_name.clone()))];
                    let (sender, listener_hook) = ListenerHook::new(table_name.clone());
                    add_listener_senders.insert(table_name.clone(), sender);
                    hook_list.push(Box::new(listener_hook));

                    tables.insert(table_name.clone(), MultiSourceTable::InHouse(table));
                    hooks.insert(table_name.clone(), hook_list);
                }
            }
        }
        let mut input_refs = vec![];
        for (name, table) in &mut tables {
            for input_table_name in table.get_input_tables() {
                input_refs.push((input_table_name.clone(), name.clone()));
            }
        }
        for (source_table, dest_table) in input_refs {
            let table_to_mod = match tables.get_mut(&source_table) {
                Some(t) => t,
                None => Err("Specified input table does not exist".to_string())?,
            };
            table_to_mod.get_output_tables().push(dest_table.clone());
        }

        let db = Database::new(tables);
        return Ok(DatabaseManager {
            db,
            hooks,
            add_listener_senders,
            workspace
        });
    }


    pub fn delete_all(
        self,
        table: &str,
        column: String,
        key: EntryValue,
    ) -> (DatabaseManager, Result<Vec<CommitedEdit>, String>){
        let mut hooks = self.hooks;
        let mut db = self.db;
        let add_listener_senders = self.add_listener_senders;
        let workspace = self.workspace;
        let result = db.delete_all(table, column, key, &mut hooks, &workspace);
        (DatabaseManager {db, hooks, add_listener_senders, workspace}, result)
    }

    pub fn insert_entry(
        self,
        table: &String,
        entry: Entry,
        source_table: Option<&str>,
    ) -> (DatabaseManager, Result<Vec<CommitedEdit>, String>) {
        let mut hooks = self.hooks;
        let mut db = self.db;
        let add_listener_senders = self.add_listener_senders;
        let workspace = self.workspace;
        let result = db.insert_entry(table, entry, source_table, &mut hooks, &workspace);
        (DatabaseManager {db, hooks, add_listener_senders, workspace}, result)
    }

    pub fn less_than_search(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        return self.db.less_than_search(table, column, key);
    }

    pub fn get_all(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        return self.db.get_all(table, column, key);
    }

    pub fn greater_than_search(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        return self.db.greater_than_search(table, column, key);
    }

    pub fn find_one(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
    ) -> Result<Option<Entry>, String> {
        return self.db.find_one(table, column, key);
    }

    #[allow(dead_code)]
    pub fn add_hook(&mut self, hook: Box<dyn Hook>, table: String) {
        if let Some(hooks) = self.hooks.get_mut(&table) {
            hooks.push(hook);
            return
        }
        self.hooks.insert(table, vec![hook]);
    }

    pub fn add_listener(&mut self, new_listener_obj:NewListenerObj, table: &str)-> Result<(), String>{
        match self.add_listener_senders.get_mut(table) {
            Some(sender) => match sender.blocking_send(new_listener_obj) {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("Error adding listener: {:?}", e.to_string()))
            },
            None => Err(format!("Unable to find listener channel for table {:?}", table))
        }
    }
}