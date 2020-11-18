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
    server::start_server()?;
    return Ok(());
}

