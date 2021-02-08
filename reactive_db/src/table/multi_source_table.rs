use crate::{config::config_reader::TableConfig, database::db_trait::HookMap, hooks::{hook::Hook, listener_hook::ListenerHook, transforms::{Transform, TransformHook}}};

use super::{storage_manager_table::StorageManagerTable, table_trait::Table, types::{Column, TableType}};

pub enum MultiSourceTable {
    InHouse(StorageManagerTable),
}

impl Table for MultiSourceTable {
    fn insert(&mut self, entry: crate::types::Entry) -> std::io::Result<Option<crate::types::Entry>> {
        match self {
            MultiSourceTable::InHouse(table) => table.insert(entry)
        }
    }

    fn delete(
        &mut self,
        search_column_name: String,
        value: &crate::types::EntryValue,
    ) -> std::io::Result<Vec<crate::types::Entry>> {
        match self {
            MultiSourceTable::InHouse(table) => table.delete(search_column_name, value)
        }
    }

    fn find_one(
        &mut self,
        search_column_name: String,
        value: &crate::types::EntryValue,
    ) -> std::io::Result<Option<crate::types::Entry>> {
        match self {
            MultiSourceTable::InHouse(table) => table.find_one(search_column_name, value)
        }
    }

    fn get_all(
        &mut self,
        search_column_name: String,
        value: crate::types::EntryValue,
    ) -> std::io::Result<Vec<crate::types::Entry>> {
        match self {
            MultiSourceTable::InHouse(table) => table.get_all(search_column_name, value)
        }
    }

    fn less_than(
        &mut self,
        search_column_name: String,
        value: crate::types::EntryValue,
        equals: bool,
    ) -> std::io::Result<Vec<crate::types::Entry>> {
        match self {
            MultiSourceTable::InHouse(table) => table.less_than(search_column_name, value, equals)
        }
    }

    fn greater_than(
        &mut self,
        search_column_name: String,
        value: crate::types::EntryValue,
    ) -> std::io::Result<Vec<crate::types::Entry>> {
        match self {
            MultiSourceTable::InHouse(table) => table.greater_than(search_column_name, value)
        }
    }

    fn get_output_tables(&mut self) -> &mut Vec<String> {
        match self {
            MultiSourceTable::InHouse(table) => table.get_output_tables()
        }
    }

    fn get_input_tables(&mut self) -> &mut Vec<String> {
        match self {
            MultiSourceTable::InHouse(table) => table.get_input_tables()
        }
    }
}