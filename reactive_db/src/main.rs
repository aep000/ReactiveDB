mod btree;
mod config;
mod constants;
mod database;
mod db_thread;
mod server;
mod table;
mod tests;
mod types;
mod networking;
mod utilities;
mod hooks;
mod database_manager;
mod storage;
mod actions;

use crate::btree::btree::BTree;
use crate::btree::node::IndexValue;
use crate::config::config_reader::read_config_file;
use crate::config::expression_parser::Expression;
use crate::database::Database;

use crate::table::{Table};
use crate::types::{Entry, EntryValue};
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    server::start_server(args[1].clone(), args[2].clone()).unwrap();
    return Ok(());
}
