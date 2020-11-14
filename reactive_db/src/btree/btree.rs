
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
    /// Creates a new Btree index by taking ownership of a storage manager
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

    pub fn delete(&mut self, index: IndexValue, reference:u32) -> io::Result<Option<Entry>>{
        self.storage_manager.start_write_session()?;

    }

    // Searches for exact values and never 
    pub fn search_exact(&mut self, index: IndexValue) ->  io::Result<Option<Entry>>{
        self.storage_manager.start_read_session()?;
        let dummy_entry = Entry {
            index: index,
            right_ref: 0,
            left_ref: None
        };
        let found_node = self.search_helper(&dummy_entry, 1)?;
        self.storage_manager.end_session();
        return match found_node.entries.binary_search(&dummy_entry){
            Ok(pos) => Ok(Some(found_node.entries[pos].clone())),
            Err(_) => Ok(None)
        };
    }

    pub fn greater_than(&mut self, index: IndexValue) -> io::Result<Vec<Entry>> {
        self.storage_manager.start_read_session()?;
        let dummy_entry = Entry {
            index: index,
            right_ref: 0,
            left_ref: None
        };
        let found_node = self.search_helper(&dummy_entry, 1)?;
        self.storage_manager.end_session();
        let start_pos = match found_node.entries.binary_search(&dummy_entry){
            Ok(pos) => pos,
            Err(pos) => pos
        };
        let mut output = vec![];
        output.extend(found_node.entries[start_pos..].to_vec());
        let mut next_node = found_node.next_node;
        while next_node != 0 {
            let current_node = self.get_node(next_node)?;
            output.extend(current_node.entries);
            next_node = current_node.next_node;
        } 

        return Ok(output);
    }

    pub fn less_than(&mut self, index: IndexValue, equals: bool) -> io::Result<Vec<Entry>> {
        let dummy_entry = Entry {
            index: index,
            right_ref: 0,
            left_ref: None
        };
        self.storage_manager.start_read_session()?;
        let mut output = vec![];
        let mut current_node = self.get_node(1)?;
        while !current_node.leaf {
            current_node = self.get_node(current_node.entries[0].left_ref.unwrap())?;
        }
        let mut pos = current_node.entries.len(); 
        while pos == current_node.entries.len() {
            pos = match current_node.entries.binary_search(&dummy_entry){
                Ok(pos) => pos,
                Err(pos) => pos
            };
            if pos < current_node.entries.len() {
                if equals{
                    output.extend(current_node.entries[..=pos].to_vec());
                }
                else{
                    output.extend(current_node.entries[..pos].to_vec());
                }
            }
            else{
                output.extend(current_node.entries);
            }
            if current_node.next_node == 0 {
                break;
            }
            current_node = self.get_node(current_node.next_node)?;
        }

        return Ok(output);
    }

    fn insert_helper(&mut self, current_node_ref:u32, entry: Entry) -> io::Result<InsertResult>{
        let mut current_node = self.get_node(current_node_ref)?;
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
            let dest_pos :isize = match current_node.entries.binary_search(&entry) {
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
    // THIS IS VERY BROKEN DO NOT USE THIS YET
    fn delete_helper(&mut self, index: &Entry, current_node_ref: u32, block_num_to_delete: Option<u32>) -> io::Result<(bool, Entry, IndexValue)>{
        let current_node = self.get_node(current_node_ref)?;
        if current_node.leaf {
            //return Ok(current_node);
        }
        let mut found_index: usize = match current_node.entries.binary_search(index){
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        if found_index == current_node.entries.len() {
            found_index = found_index-1;
        }
        // Left side
        let (should_rebalance, deleted_entry, new_median) = match index < &current_node.entries[found_index as usize]{
            true => self.delete_helper(index, current_node.entries[found_index as usize].left_ref.unwrap(), block_num_to_delete)?,
        // Right side
            false => self.delete_helper(index, current_node.entries[found_index as usize].right_ref, block_num_to_delete)?
        };
        if should_rebalance {
            if current_node.entries.len() == 1 {
                self.storage_manager.delete_data(current_node_ref);
                return Ok((true, deleted_entry, IndexValue::Integer(0)));
            }
            else{
                // No need to re-assign left and right ref if removal was on right most node
                if current_node.entries.len()-1 == found_index {
                    if index < &current_node.entries[found_index as usize] {
                        current_node.entries[found_index as usize -1].right_ref = current_node.entries[found_index as usize].right_ref;
                        current_node.entries.remove(found_index);
                    }
                    else {
                        current_node.entries.remove(found_index);
                    }
                }
                else if found_index == 0 {
                    if index < &current_node.entries[found_index as usize] {
                        current_node.entries.remove(found_index);
                    }
                    else {
                        current_node.entries[found_index as usize -1].right_ref = current_node.entries[found_index as usize].right_ref;
                        current_node.entries.remove(found_index);
                    }
                }
                else{
                    current_node.entries[found_index+1 as usize].right_ref = current_node.entries[found_index-1 as usize].left_ref.unwrap();
                    current_node.entries.remove(found_index);
                }
            }
        }
        self.storage_manager.delete_data(current_node_ref);
        self.storage_manager.write_data(serde_json::to_vec(&current_node)?, Some(current_node_ref));
        return Ok((false, deleted_entry, current_node.entries[(found_index as usize)/2].index));
    }

    fn search_helper(&mut self, index: &Entry, current_node_ref: u32) -> io::Result<Node>{
        let current_node = self.get_node(current_node_ref)?;
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


    fn get_node(&mut self, location: u32) -> io::Result<Node>{
        let current_node_raw = self.storage_manager.read_data(location)?;
        let current_node: Result<Node> = serde_json::from_reader(Cursor::new(current_node_raw));
        return match current_node{
            Ok(node) => Ok(node),
            Err(error) =>  Err(Error::new(ErrorKind::Other, "Error Decoding Node"))
        };
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