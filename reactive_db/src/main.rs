mod storage_manager;
mod btree;
mod table;
mod config_reader;
mod parser;
mod types;
mod database;
mod transform;


use std::collections::HashMap;
use crate::database::Database;
use crate::transform::Transform;
use crate::parser::Statement;
use crate::parser::Expression;
use crate::config_reader::read_config_file;
use std::collections::BTreeMap;
use crate::table::{Table, TableType, Column};
use crate::types::{DataType, EntryValue};
use std::time::Instant;
use rand::distributions::Uniform;
use crate::btree::node::IndexValue;
use crate::storage_manager::StorageManager;
use std::io;
use crate::btree::btree::BTree;

fn main() -> io::Result<()>{
    /*
    let mut columns = vec![];
    columns.push(Column::new("first_column".to_string(), DataType::Integer));
    columns.push(Column::new("second_column".to_string(), DataType::Str));

    let mut test_table = Table::new("TestOne".to_string(), columns, TableType::Source)?;
    /*let mut new_entry = EntryBuilder::new();
    new_entry.column("first_column", EntryValue::Integer(15));
    new_entry.column("second_column", EntryValue::Str("B".to_string()));

    let mut next_entry = EntryBuilder::new();
    next_entry.column("first_column", EntryValue::Integer(13));
    next_entry.column("second_column", EntryValue::Str("D".to_string()));
    
    test_table.insert(new_entry.build())?;
    test_table.insert(next_entry.build())?;*/
    println!("{:?}", test_table.exact_get("first_column".to_string(), EntryValue::Integer(12)));
    //println!("{:?}", test_table.exact_get("first_column".to_string(), EntryValue::Integer(13)));
    println!("{:?}", test_table.exact_get("second_column".to_string(), EntryValue::Str("Test".to_string())));
    println!("{:?}", test_table.less_than("first_column".to_string(), EntryValue::Integer(14), true));
*/
    //print!("{:?}",read_config_file("test_cfg.yaml".to_string()));

    let config = read_config_file("test_cfg.yaml".to_string())?;
    let mut db = Database::from_config(config).unwrap();
    let mut entry_to_insert = BTreeMap::new();
    entry_to_insert.insert("something".to_string(), EntryValue::Integer(2));
    print!("{:?}\n", db.insert_entry(&"something".to_string(),entry_to_insert));

    print!("{:?}\n", db.greater_than_search(&"derived".to_string(), "newtable".to_string(), EntryValue::Integer(0)));

    return Ok(());
}
#[derive(Clone)]
struct EntryBuilder {
    map: BTreeMap<String, EntryValue>
}

impl EntryBuilder {
    pub fn new() -> EntryBuilder{
        return EntryBuilder {
            map: BTreeMap::new()
        }
    }
    pub fn column(&mut self, key: &str, value: EntryValue) -> EntryBuilder{
        self.map.insert(key.to_string(), value);
        return self.clone();
    }
    pub fn build(&mut self) -> BTreeMap<String, EntryValue> {
        self.map.clone()
    }
}
