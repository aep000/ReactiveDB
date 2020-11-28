mod btree;
mod client_connection;
mod config;
mod constants;
mod database;
mod db_thread;
mod server;
mod storage_manager;
mod table;
mod tests;
mod transform;
mod types;
mod network_types;
mod utilities;

use crate::btree::btree::BTree;
use crate::btree::node::IndexValue;
use crate::config::config_reader::read_config_file;
use crate::config::parser::Expression;
use crate::database::Database;
use crate::storage_manager::StorageManager;
use crate::table::{Column, Table, TableType};
use crate::transform::Transform;
use crate::types::{DataType, Entry, EntryValue};
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    server::start_server(args[1].clone(), args[2].clone()).unwrap();
    return Ok(());
}
