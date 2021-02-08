mod btree;
mod config;
mod constants;
mod server;
mod table;
mod tests;
mod types;
mod networking;
mod utilities;
mod hooks;
mod storage;
mod actions;
mod database;

use crate::btree::btree::BTree;
use crate::btree::node::IndexValue;

use crate::config::expression_parser::Expression;

use crate::types::{Entry, EntryValue};
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    server::start_server(args[1].clone(), args[2].clone()).unwrap();
    return Ok(());
}
