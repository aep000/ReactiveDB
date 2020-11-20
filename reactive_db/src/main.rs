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

use std::time::Duration;
use crate::btree::btree::BTree;
use crate::btree::node::IndexValue;
use crate::config::config_reader::read_config_file;
use crate::database::Database;
use crate::config::parser::{Expression};
use crate::storage_manager::StorageManager;
use crate::table::{Column, Table, TableType};
use crate::transform::Transform;
use crate::types::{DataType, EntryValue, Entry};
use std::io;
use std::env;
use std::thread;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    server::start_server(args[1].clone(), args[2].clone()).unwrap();
    return Ok(());
}