mod storage_manager;
mod btree;
use crate::btree::node::Node;
use serde_json::{Result};


use std::io::Cursor;
use std::time::Instant;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use crate::btree::node::IndexValue;
use crate::storage_manager::StorageManager;
use std::io;
use crate::btree::btree::BTree;
use rand::{thread_rng, Rng};

fn main() -> io::Result<()>{
    let storage_manager: StorageManager = StorageManager::new("/Users/alexparson/Projects/ReactiveDB/reactive_db/benchmark.index".to_string())?;

    let mut tree = BTree::new(5, storage_manager)?;
    
    let between = Uniform::from(10..40000);
    let mut rng = rand::thread_rng();
    
    let start = Instant::now();
   /* 
    for n in 0..20000 {
        //println!("Inserting: {}", n);
        tree.insert(IndexValue::Integer(between.sample(&mut rng)), n)?;
    }
    */

    print!("\n\n FOUND RESULT:{:?}", tree.search_exact(IndexValue::Integer(15236)));

    println!("Operation took {} seconds!", start.elapsed().as_secs());

    
    /*tree.insert(IndexValue::String("Alex".to_string()), 101)?;
    tree.insert(IndexValue::String("Sean".to_string()), 102)?;
    tree.insert(IndexValue::String("John".to_string()), 103)?;
    tree.insert(IndexValue::String("Dave".to_string()), 104)?;
    tree.insert(IndexValue::String("Luke".to_string()), 105)?;
    tree.insert(IndexValue::String("Jamie".to_string()), 106)?;*/





    return Ok(());
}