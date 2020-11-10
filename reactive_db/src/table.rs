
use crate::types::create_custom_io_error;
use crate::types::EntryValue;
use crate::BTree;
use crate::types::DataType;
use std::collections::BTreeMap;
use crate::StorageManager;
use std::collections::HashMap;
use std::io;
use serde_json::Result;

const BTREE_NODE_SIZE: u32 = 20;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq,)]
pub struct Column {
    data_type: DataType,
    name: String,
    indexed: bool,
    index_loc: usize
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq,)]
pub enum TableType {
    Source,
    Derived(Transform)
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq,)]
pub enum Transform {
    Filter,
    Union,
    Function,
    Aggregate
}

pub struct Table {
    name: String,
    columns: HashMap<String, Column>,
    table_type: TableType,
    output_tables: Vec<Table>,
    indexes: Vec<BTree>,
    entry_storage_manager: StorageManager
}

impl Column {
    pub fn new(name: String, data_type:DataType) -> Column{
        Column {
            data_type: data_type,
            name: name,
            indexed: false,
            index_loc: 0
        }
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>, table_type: TableType, ) -> io::Result<Table> {
        let entry_storage_manager = StorageManager::new(format!("db/{}.db", name))?;
        let mut indexes = vec![];
        let mut column_map = HashMap::new();
        for column in &columns {
            let mut column = column.clone();
            if column.data_type.is_indexible(){
                let file_name = format!("db/{}_{}.index", name, column.name);
                let storage_manager = StorageManager::new(file_name)?;
                column.indexed = true;
                column.index_loc = indexes.len();
                indexes.push(BTree::new(BTREE_NODE_SIZE, storage_manager)?);
            }
            column_map.insert(column.name.clone(), column);
        }
        return Ok(Table {
            name: name,
            columns: column_map,
            table_type: table_type,
            output_tables: vec![],
            indexes: indexes,
            entry_storage_manager: entry_storage_manager
        });
    }
    pub fn insert(&mut self, entry: BTreeMap<String, EntryValue>) -> io::Result<()>{
        self.entry_storage_manager.start_write_session()?;
        let reserved_root = self.entry_storage_manager.allocate_block();
        for (name, val) in &entry{
            match self.columns.get(name){
                Some(column) => {
                    if column.indexed{
                        self.indexes[column.index_loc].insert(val.to_index_value()?, reserved_root)?;
                    }
                }
                None=> {
                    return Err(create_custom_io_error("Missmatched Input"));
                }
            };
        }
        self.entry_storage_manager.write_data(serde_json::to_vec(&entry)?, Some(reserved_root))?;
        self.entry_storage_manager.end_session();
        Ok(())
    }

    pub fn exact_get(&mut self, search_column_name: String, value: EntryValue) -> io::Result<BTreeMap<String, EntryValue>>{
        let column = match self.columns.get(&search_column_name){
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(format!("No such column {} exists", search_column_name).as_str()))
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(format!("No such column {} exists", search_column_name).as_str()));
        }
        let location_ref = self.indexes[column.index_loc].search_exact(value.to_index_value()?)?;
        self.entry_storage_manager.start_read_session()?;
        let raw_entry = self.entry_storage_manager.read_data(location_ref.right_ref)?;
        self.entry_storage_manager.end_session();
        let entry: Result<BTreeMap<String, EntryValue>>= serde_json::from_slice(raw_entry.as_slice());
        return match entry {
            Ok(tree) => Ok(tree),
            Err(e) => Err(create_custom_io_error(format!("{:?}", e).as_str()))
        };
    }

    pub fn less_than(&mut self, search_column_name: String, value: EntryValue, equals: bool) -> io::Result<Vec<BTreeMap<String, EntryValue>>>{
        let column = match self.columns.get(&search_column_name){
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(format!("No such column {} exists", search_column_name).as_str()))
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(format!("No such column {} exists", search_column_name).as_str()));
        }
        let location_refs = self.indexes[column.index_loc].less_than(value.to_index_value()?, equals)?;
        self.entry_storage_manager.start_read_session()?;
        let mut output = vec![];
        for location_ref in location_refs{
            let raw_entry = self.entry_storage_manager.read_data(location_ref.right_ref)?;
            let entry: Result<BTreeMap<String, EntryValue>>= serde_json::from_slice(raw_entry.as_slice());
            let entry_unwrapped =  match entry {
                Ok(tree) => Ok(tree),
                Err(e) => Err(create_custom_io_error(format!("{:?}", e).as_str()))
            }?;
            output.push(entry_unwrapped);
        }
        self.entry_storage_manager.end_session();
        return Ok(output)
    }

    pub fn greater_than(&mut self, search_column_name: String, value: EntryValue) -> io::Result<Vec<BTreeMap<String, EntryValue>>>{
        let column = match self.columns.get(&search_column_name){
            Some(c) => Ok(c),
            None => Err(create_custom_io_error(format!("No such column {} exists", search_column_name).as_str()))
        }?;
        if !column.indexed {
            return Err(create_custom_io_error(format!("No such column {} exists", search_column_name).as_str()));
        }
        let location_refs = self.indexes[column.index_loc].greater_than(value.to_index_value()?)?;
        self.entry_storage_manager.start_read_session()?;
        let mut output = vec![];
        for location_ref in location_refs{
            let raw_entry = self.entry_storage_manager.read_data(location_ref.right_ref)?;
            let entry: Result<BTreeMap<String, EntryValue>>= serde_json::from_slice(raw_entry.as_slice());
            let entry_unwrapped =  match entry {
                Ok(tree) => Ok(tree),
                Err(e) => Err(create_custom_io_error(format!("{:?}", e).as_str()))
            }?;
            output.push(entry_unwrapped);
        }
        self.entry_storage_manager.end_session();
        return Ok(output)
    }
}