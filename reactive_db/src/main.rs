mod btree;
mod config_reader;
mod database;
mod parser;
mod storage_manager;
mod table;
mod transform;
mod types;
mod constants;
mod tests;

use crate::btree::btree::BTree;
use crate::btree::node::IndexValue;
use crate::config_reader::read_config_file;
use crate::database::Database;
use crate::parser::Expression;
use crate::parser::Statement;
use crate::storage_manager::StorageManager;
use crate::table::{Column, Table, TableType};
use crate::transform::Transform;
use crate::types::{DataType, EntryValue};
use rand::Rng;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::time::Instant;

fn main() -> io::Result<()> {
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
    let mut db = Database::from_config(config, "db/".to_string()).unwrap();
    let arr = vec![0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,];
    let mut rng = rand::thread_rng();
    for n in arr {
        let mut entry_to_insert = EntryBuilder::new();
        let i = rng.gen_range(0.0, 10.0) as isize;
        entry_to_insert.column("testForIteration", EntryValue::Integer(n));
        entry_to_insert.column("testForIndex", EntryValue::Integer(i));
        print!(
            "{}, {}: {:?}\n",
            n,
            i,
            db.insert_entry(&"testTable".to_string(), entry_to_insert.build())
        );
    }
    
    
    print!(
        "{:?}\n",
        db.delete_all(
            &"testTable".to_string(),
            "testForIndex".to_string(),
            EntryValue::Integer(8)
        )
    );

    print!(
        "{:?}\n",
        db.greater_than_search(
            &"testTable".to_string(),
            "testForIndex".to_string(),
            EntryValue::Integer(6)
        )
    );

    return Ok(());
}
#[derive(Clone)]
struct EntryBuilder {
    map: BTreeMap<String, EntryValue>,
}

impl EntryBuilder {
    pub fn new() -> EntryBuilder {
        return EntryBuilder {
            map: BTreeMap::new(),
        };
    }
    pub fn column(&mut self, key: &str, value: EntryValue) -> EntryBuilder {
        self.map.insert(key.to_string(), value);
        return self.clone();
    }
    pub fn build(&mut self) -> BTreeMap<String, EntryValue> {
        self.map.clone()
    }
}
