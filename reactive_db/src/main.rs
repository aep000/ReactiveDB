mod btree;
mod database;
mod storage_manager;
mod table;
mod transform;
mod types;
mod constants;
mod tests;
mod config;
mod server;

use crate::server::{DBRequest, InsertData, GetData};
use std::sync::mpsc::channel;
use crate::btree::btree::BTree;
use crate::btree::node::IndexValue;
use crate::config::config_reader::read_config_file;
use crate::database::Database;
use crate::config::parser::{Statement, Expression};
use crate::storage_manager::StorageManager;
use crate::table::{Column, Table, TableType};
use crate::transform::Transform;
use crate::types::{DataType, EntryValue, Entry};
use rand::Rng;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::time::Instant;
use std::thread;
use std::time::Duration;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

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

    //print!("{:?}",read_config_file("test_cfg.yaml".to_string()));
    let request = GetData {
        column: "testForIndex".to_string(),
        table: "testTable".to_string(),
        key: EntryValue::Integer(0)
    };

    let request = DBRequest::FindOne(request);
    let mut serialized_request = serde_json::to_string(&request).unwrap();
    let mut total_request: Vec<u8> = vec![];
    total_request.write_u32::<BigEndian>(serialized_request.len() as u32)?;
    let mut bytes = serialized_request.into_bytes();
    total_request.append(&mut bytes);
    print!("{}\n", String::from_utf8(total_request).unwrap());

    let mut entry_builder = EntryBuilder::new();
    entry_builder.column("testForIndex", EntryValue::Integer(0));
    entry_builder.column("testForIteration", EntryValue::Integer(1));

    let request = InsertData {
        table: "testTable".to_string(),
        entry: entry_builder.build()
    };

    let request = DBRequest::InsertData(request);
    let mut serialized_request = serde_json::to_string(&request).unwrap();
    let mut total_request: Vec<u8> = vec![];
    total_request.write_u32::<BigEndian>(serialized_request.len() as u32)?;
    let mut bytes = serialized_request.into_bytes();
    total_request.append(&mut bytes);
    print!("{}\n", String::from_utf8(total_request).unwrap());
    */
    server::start_server()?;

    return Ok(());
}
#[derive(Clone)]
struct EntryBuilder {
    map: Entry,
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
    pub fn build(&mut self) -> Entry {
        self.map.clone()
    }
}
