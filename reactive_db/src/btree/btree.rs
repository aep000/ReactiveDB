
use serde_json::{Result};
use std::io::{Error, ErrorKind, Cursor};
use std::io;

use crate::btree::node::{Node, IndexValue, Entry};
use crate::StorageManager;

pub struct BTree {
    node_size: u32,
    storage_manager: StorageManager,
}

pub enum InsertResult {
    Normal,
    Rebalance(u32, IndexValue, u32)
}

impl BTree {
    pub fn new(node_size: u32, mut storage_manager: StorageManager) -> io::Result<BTree>{
        storage_manager.start_write_session()?;
        if storage_manager.is_empty(1)? {
            let root_node = Node {
                leaf: true,
                entries: vec![],
                next_node: 0
            };
            let root_node_encoded = unwrap_encode(&root_node);
            storage_manager.write_data(root_node_encoded, Some(1))?;
        }
        storage_manager.end_session();
        return Ok(BTree {
            node_size: node_size,
            storage_manager: storage_manager
        });
    }

    pub fn insert(&mut self, index: IndexValue, reference:u32) -> io::Result<()>{
        self.storage_manager.start_write_session()?;
        let entry = Entry {
            index: index,
            left_ref:None,
            right_ref: reference
        };
        let result = self.insert_helper(1, entry)?;
        match result {
            InsertResult::Normal => {},
            InsertResult::Rebalance(mut left, index, right) => {
                if left == 1 {
                    let temp_old_root = self.storage_manager.read_data(1)?;
                    self.storage_manager.delete_data(1)?;
                    let location = self.storage_manager.write_data(temp_old_root, None)?;
                    left = location;
                }
                let new_entry = Entry{
                    left_ref: Some(left),
                    index: index,
                    right_ref: right
                };
                let new_node = Node{
                    leaf: false,
                    entries: vec![new_entry],
                    next_node: 0
                };
                self.storage_manager.write_data(unwrap_encode(&new_node), Some(1))?;

            }
        }
        self.storage_manager.end_session();
        return Ok(());
    }
    fn insert_helper(&mut self, current_node_ref:u32, entry: Entry) -> io::Result<InsertResult>{
        //println!("Looking to insert in {}", current_node_ref);
        let current_node_raw = self.storage_manager.read_data(current_node_ref)?;
        let current_node: Result<Node> = serde_json::from_reader(Cursor::new(current_node_raw));
        let mut current_node =  match current_node {
            Ok (value) => value,
            error =>{
                panic!("Error decoding node {:?}", error)
            }
        };
        //Is this a leaf node?
        if current_node.leaf {
            //Are there too many entries?
            if current_node.entries.len() >= self.node_size as usize {
                let median_index = current_node.entries.len()/2;
                let mut left = current_node.entries[..median_index].to_vec();
                let mut right = current_node.entries[median_index..].to_vec();
                let median_value = &current_node.entries[median_index];
                if entry < *median_value {
                    insert_entry(&entry, &mut left);
                }
                else{
                    insert_entry(&entry, &mut right);
                }
                self.storage_manager.delete_data(current_node_ref)?;

                // Get New Block
                let mut left_block = self.storage_manager.allocate_block();
                if left_block == 1{
                    left_block = self.storage_manager.allocate_block();
                }

                let mut right_block = self.storage_manager.allocate_block();
                if right_block == 1{
                    right_block = self.storage_manager.allocate_block();
                }

                let left_node = Node {
                    entries: left,
                    next_node: right_block,
                    leaf: true
                };

                let right_node = Node {
                    entries: right,
                    next_node: current_node.next_node,
                    leaf: true
                };
                let encoded_left = unwrap_encode(&left_node);
                let encoded_right = unwrap_encode(&right_node);
                if left_block == 1 || right_block == 1{
                    println!("Left or right are zero");
                }

                self.storage_manager.write_data(encoded_left, Some(left_block))?;
                self.storage_manager.write_data(encoded_right, Some(right_block))?;
                return Ok(InsertResult::Rebalance(left_block, median_value.index.clone(), right_block));
            }
            else{
                insert_entry(&entry, &mut current_node.entries);
                let node_to_write = unwrap_encode(&current_node);
                self.storage_manager.delete_data(current_node_ref)?;
                self.storage_manager.write_data(node_to_write, Some(current_node_ref))?;
                return Ok(InsertResult::Normal)
            }
        }
        else{
            let mut dest_pos :isize = match current_node.entries.binary_search(&entry) {
                Ok(pos) => pos as isize,
                Err(pos) => pos as isize - 1
            };
            let index = match dest_pos == -1 {
                true => (1+ dest_pos),
                false => dest_pos
            };
            let mut next_node_ref = current_node.entries[index as usize].right_ref;
            if dest_pos == -1 {
                next_node_ref = current_node.entries[index as usize].left_ref.unwrap();
            }

            let sub_results = self.insert_helper(next_node_ref, entry)?;
            return match sub_results {
                InsertResult::Normal => Ok(InsertResult::Normal),
                InsertResult::Rebalance(left_ref, result_index, right_ref) => {
                    let new_entry = Entry {
                        index: result_index,
                        left_ref: Some(left_ref),
                        right_ref: right_ref
                    };
                    if current_node.entries.len() >= self.node_size as usize {
                        let median_index = current_node.entries.len()/2;
                        let mut left = current_node.entries[..median_index].to_vec();
                        let mut right = current_node.entries[median_index..].to_vec();
                        let median_value = &current_node.entries[median_index];
                        if new_entry < *median_value {
                            insert_non_leaf_entry(&new_entry, &mut left);
                        }
                        else{
                            insert_non_leaf_entry(&new_entry, &mut right);
                        }
                        self.storage_manager.delete_data(current_node_ref)?;

                        // Get New Block
                        let mut left_block = self.storage_manager.allocate_block();
                        if left_block == 1{
                            left_block = self.storage_manager.allocate_block();
                        }

                        let mut right_block = self.storage_manager.allocate_block();
                        if right_block == 1{
                            right_block = self.storage_manager.allocate_block();
                        }

                        let left_node = Node {
                            entries: left,
                            next_node: 0,
                            leaf: false
                        };

                        let right_node = Node {
                            entries: right,
                            next_node: 0,
                            leaf: false
                        };
                        let encoded_left = unwrap_encode(&left_node);
                        let encoded_right = unwrap_encode(&right_node);
                        if left_block == 1 || right_block == 1{
                            println!("Left or right are zero");
                        }

                        self.storage_manager.write_data(encoded_left, Some(left_block))?;
                        self.storage_manager.write_data(encoded_right, Some(right_block))?;
                        return Ok(InsertResult::Rebalance(left_block, median_value.index.clone(), right_block));
                    }
                    else{
                        insert_non_leaf_entry(&new_entry, &mut current_node.entries);
                        
                        self.storage_manager.delete_data(current_node_ref)?;
                        self.storage_manager.write_data(unwrap_encode(&current_node), Some(current_node_ref))?;

                        return Ok(InsertResult::Normal);
                    }
                }
            };
        }
    }



    // Searches for exact values and never 
    pub fn search_exact(&mut self, index: IndexValue) ->  io::Result<Entry>{
        self.storage_manager.start_read_session()?;
        let dummy_entry = Entry {
            index: index,
            right_ref: 0,
            left_ref: None
        };
        let found_node = self.search_helper(&dummy_entry, 1)?;
        self.storage_manager.end_session();
        return match found_node.entries.binary_search(&dummy_entry){
            Ok(pos) => Ok(found_node.entries[pos].clone()),
            Err(_) => Err(Error::new(ErrorKind::Other, "Unable to find result"))
        };
    }
    fn search_helper(&mut self, index: &Entry, current_node_ref: u32) -> io::Result<Node>{
        let current_node_raw = self.storage_manager.read_data(current_node_ref)?;
        let current_node: Result<Node> = serde_json::from_reader(Cursor::new(current_node_raw));
        let current_node = match current_node{
            Ok(node) => node,
            Err(error) => {
                panic!("Something went wrong {:?}", error);
            }
        };
        if current_node.leaf {
            return Ok(current_node);
        }
        let mut found_index: usize = match current_node.entries.binary_search(index){
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        if found_index == current_node.entries.len() {
            found_index = found_index-1;
        }
        if index < &current_node.entries[found_index as usize]{
            return self.search_helper(index, current_node.entries[found_index as usize].left_ref.unwrap());
        }
        return self.search_helper(index, current_node.entries[found_index as usize].right_ref);
    }
}

fn insert_entry(entry: &Entry, destination: &mut Vec<Entry>) -> usize{
    match destination.binary_search(&entry) {
        Ok(pos) => {
            destination.insert(pos, entry.clone());
            return pos;
        },
        Err(pos) => {
            destination.insert(pos, entry.clone());
            return pos;
        }
    };
}

fn unwrap_encode(node: &Node) -> Vec<u8>{
    return match serde_json::to_vec(&node){
        Ok (value) => value,
        _error =>{
            panic!("Error decoding node")
        }
    };
}

fn insert_non_leaf_entry(entry: &Entry, destination: &mut Vec<Entry>) -> usize {
    let location = insert_entry(&entry, destination);
    if location > 0{
        destination[location-1].right_ref = entry.left_ref.unwrap();
    }
    if location < destination.len()-1{
        destination[location+1].left_ref = Some(entry.right_ref);
    }
    return location;
}


fn print_raw_stored_data(data: &Vec<u8>){
    print!("\n{:?}",std::str::from_utf8(data.as_slice()));
}