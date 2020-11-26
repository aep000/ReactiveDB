use crate::constants::{BTREE_NODE_SIZE, ROW_ID_COLUMN_NAME};
use crate::transform::Transform;
use crate::types::create_custom_io_error;
use crate::types::DataType;
use crate::types::{Entry, EntryValue};
use crate::BTree;
use crate::StorageManager;
use serde_json::Result;
use std::collections::HashMap;
use std::io;
use uuid::Uuid;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
pub struct Column {
    data_type: DataType,
    name: String,
    indexed: bool,
    index_loc: usize,
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
pub enum TableType {
    Source,
    Derived(Transform),
}

pub struct Table {
    pub name: String,
    columns: HashMap<String, Column>,
    pub table_type: TableType,
    pub output_tables: Vec<String>,
    pub input_tables: Vec<String>,
    indexes: Vec<BTree>,
    entry_storage_manager: StorageManager,
    path: String,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Column {
        Column {
            data_type: data_type,
            name: name,
            indexed: false,
            index_loc: 0,
        }
    }
}

impl Table {
    pub fn new(
        table_name: String,
        columns: Vec<Column>,
        table_type: TableType,
        file_path: String,
    ) -> io::Result<Table> {
        let mut entry_storage_manager =
            StorageManager::new(format!("{}/{}.db", file_path, table_name))?;
        let mut indexes = vec![];
        let mut column_map = HashMap::new();
        for column in &columns {
            let mut column = column.clone();
            if column.data_type.is_indexible() {
                let file_name = format!("{}/{}_{}.index", file_path, table_name, column.name);
                let storage_manager = StorageManager::new(file_name)?;
                column.indexed = true;
                column.index_loc = indexes.len();
                indexes.push(BTree::new(BTREE_NODE_SIZE, storage_manager)?);
            }
            column_map.insert(column.name.clone(), column);
        }
        match table_type {
            TableType::Derived(_) => {
                entry_storage_manager.start_read_session()?;
                match entry_storage_manager.read_data(2) {
                    Ok(raw_entry) => {
                        entry_storage_manager.end_session();
                        let entry: Result<Entry> = serde_json::from_slice(raw_entry.as_slice());
                        match entry {
                            Ok(entry_unwrapped) => {
                                for (column_name, value) in entry_unwrapped {
                                    let data_type = get_data_type_of_entry(&value);
                                    let mut column = Column::new(column_name.clone(), data_type);
                                    if column.data_type.is_indexible() {
                                        let file_name = format!(
                                            "{}/{}_{}.index",
                                            file_path, table_name, column.name
                                        );
                                        let storage_manager = StorageManager::new(file_name)?;
                                        column.indexed = true;
                                        column.index_loc = indexes.len();
                                        indexes.push(BTree::new(BTREE_NODE_SIZE, storage_manager)?);
                                    }
                                    column_map.insert(column.name.clone(), column);
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(_) => {}
                }
            }
            _ => {}
        }
        return Ok(Table {
            name: table_name,
            columns: column_map,
            table_type: table_type,
            output_tables: vec![],
            input_tables: vec![],
            indexes: indexes,
            entry_storage_manager: entry_storage_manager,
            path: file_path,
        });
    }
    pub fn insert(&mut self, mut entry: Entry) -> io::Result<Option<Entry>> {
        entry.insert(
            ROW_ID_COLUMN_NAME.to_string(),
            EntryValue::ID(Uuid::new_v4().to_hyphenated().to_string()),
        );
        self.entry_storage_manager.start_write_session()?;
        let reserved_root = self.entry_storage_manager.allocate_block();
        for (name, val) in &entry {
            match self.columns.get(name) {
                Some(column) => {
                    if column.indexed {
                        self.indexes[column.index_loc]
                            .insert(val.to_index_value()?, reserved_root)?;
                    }
                }
                None => match self.table_type {
                    TableType::Derived(_) => {
                        let new_column = Column::new(name.to_string(), get_data_type_of_entry(val));
                        let path = self.path.clone();
                        self.create_new_index(new_column, &path)?;
                        match self.columns.get(name) {
                            Some(column) => {
                                if column.indexed {
                                    self.indexes[column.index_loc]
                                        .insert(val.to_index_value()?, reserved_root)?;
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => Err(create_custom_io_error("Missmatched Input"))?,
                },
            };
        }
        self.entry_storage_manager
            .write_data(serde_json::to_vec(&entry)?, Some(reserved_root))?;
        self.entry_storage_manager.end_session();
        Ok(Some(entry))
    }

    pub fn delete(
        &mut self,
        search_column_name: String,
        value: &EntryValue,
    ) -> io::Result<Vec<Entry>> {
        let search_column = match self.columns.get(&search_column_name) {
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            )),
        }?;
        if !search_column.indexed {
            return Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            ));
        }
        let location_refs =
            self.indexes[search_column.index_loc].delete(value.to_index_value()?, None, true)?;
        let mut deleted_entries = vec![];
        for loc in location_refs {
            self.entry_storage_manager.start_write_session()?;
            let raw_entry = self.entry_storage_manager.read_data(loc)?;
            self.entry_storage_manager.delete_data(loc)?;
            self.entry_storage_manager.end_session();
            let entry: Result<Entry> = serde_json::from_slice(raw_entry.as_slice());
            match entry {
                Ok(entry) => {
                    for (column_name, itered_column) in self.columns.iter() {
                        if itered_column.indexed {
                            match entry.get(column_name) {
                                Some(value) => self.indexes[itered_column.index_loc].delete(
                                    value.to_index_value()?,
                                    Some(loc),
                                    false,
                                )?,
                                _ => vec![],
                            };
                        }
                    }
                    deleted_entries.push(entry);
                }
                Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?,
            }
        }
        Ok(deleted_entries)
    }

    pub fn find_one(
        &mut self,
        search_column_name: String,
        value: &EntryValue,
    ) -> io::Result<Option<Entry>> {
        let column = match self.columns.get(&search_column_name) {
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            )),
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            ));
        }
        let location_ref = self.indexes[column.index_loc].search_exact(value.to_index_value()?)?;
        match location_ref {
            Some(location_ref) => {
                self.entry_storage_manager.start_read_session()?;
                let raw_entry = self
                    .entry_storage_manager
                    .read_data(location_ref.right_ref)?;
                self.entry_storage_manager.end_session();
                let entry: Result<Entry> = serde_json::from_slice(raw_entry.as_slice());
                return match entry {
                    Ok(tree) => Ok(Some(tree)),
                    Err(e) => Err(create_custom_io_error(format!("{:?}", e).as_str())),
                };
            }
            None => Ok(None),
        }
    }

    pub fn get_all(
        &mut self,
        search_column_name: String,
        value: EntryValue,
    ) -> io::Result<Vec<Entry>> {
        let column = match self.columns.get(&search_column_name) {
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            )),
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            ));
        }
        let location_refs =
            self.indexes[column.index_loc].get_all(value.to_index_value()?)?;
        self.entry_storage_manager.start_read_session()?;
        let mut output = vec![];
        for location_ref in location_refs {
            let entry = self.get_entry(location_ref.right_ref)?;
            output.push(entry);
        }
        self.entry_storage_manager.end_session();
        return Ok(output);
    }

    pub fn less_than(
        &mut self,
        search_column_name: String,
        value: EntryValue,
        equals: bool,
    ) -> io::Result<Vec<Entry>> {
        let column = match self.columns.get(&search_column_name) {
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            )),
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            ));
        }
        let location_refs =
            self.indexes[column.index_loc].less_than(value.to_index_value()?, equals)?;
        self.entry_storage_manager.start_read_session()?;
        let mut output = vec![];
        for location_ref in location_refs {
            let entry = self.get_entry(location_ref.right_ref)?;
            output.push(entry);
        }
        self.entry_storage_manager.end_session();
        return Ok(output);
    }

    pub fn greater_than(
        &mut self,
        search_column_name: String,
        value: EntryValue,
    ) -> io::Result<Vec<Entry>> {
        let column = match self.columns.get(&search_column_name) {
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            )),
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(
                format!("No such column {} exists", search_column_name).as_str(),
            ));
        }
        let location_refs = self.indexes[column.index_loc].greater_than(value.to_index_value()?)?;
        self.entry_storage_manager.start_read_session()?;
        let mut output = vec![];
        for location_ref in location_refs {
            let entry = self.get_entry(location_ref.right_ref)?;
            output.push(entry);
        }
        self.entry_storage_manager.end_session();
        return Ok(output);
    }

    fn create_new_index(&mut self, mut column: Column, file_path: &String) -> io::Result<()> {
        if column.data_type.is_indexible() {
            let file_name = format!("{}/{}_{}.index", file_path, self.name, column.name);
            let storage_manager = StorageManager::new(file_name)?;
            column.indexed = true;
            column.index_loc = self.indexes.len();
            self.indexes
                .push(BTree::new(BTREE_NODE_SIZE, storage_manager)?);
        }
        self.columns.insert(column.name.clone(), column);
        return Ok(());
    }

    fn get_entry(&mut self, location_ref: u32) -> io::Result<Entry>{
        self.entry_storage_manager.start_read_session()?;
        let raw_entry = self
            .entry_storage_manager
            .read_data(location_ref)?;
        self.entry_storage_manager.end_session();
        let entry: Result<Entry> = serde_json::from_slice(raw_entry.as_slice());
        return match entry {
            Ok(tree) => Ok(tree),
            Err(e) => Err(create_custom_io_error(format!("{:?}", e).as_str())),
        };
    }
}

fn get_data_type_of_entry(entry: &EntryValue) -> DataType {
    return match entry {
        EntryValue::Array(data) => DataType::Array(Box::new(get_data_type_of_entry(&data[0]))),
        EntryValue::Integer(_) => DataType::Integer,
        EntryValue::Str(_) => DataType::Str,
        EntryValue::Bool(_) => DataType::Bool,
        EntryValue::Map(data) => {
            let mut output = vec![];
            for (key, v) in data {
                output.push((key.clone(), get_data_type_of_entry(v)))
            }
            return DataType::Map(output);
        }
        EntryValue::ID(_) => DataType::ID,
    };
}
