mod storage_manager;

use crate::storage_manager::StorageManager;
use std::io;
use std::collections::BinaryHeap;

fn main() -> io::Result<()>{
    let mut storage_manager: StorageManager = StorageManager {
        file_name: "test.index".to_string(),
        open_blocks: BinaryHeap::new(),
        number_of_blocks: 0,
        session_open: false,
        open_file: None
    };
    storage_manager.start_write_session()?;

    //storage_manager.write_block(1, "This is a test".as_bytes().to_vec())?;
    //storage_manager.write_block(2, "This is a second test".as_bytes().to_vec())?;
    storage_manager.write_data("This is a second test".as_bytes().to_vec(), None)?;
    let first_block = storage_manager.read_data(1)?;

    let first_block_str = String::from_utf8(first_block).expect("something went wrong");
    println!("{}", first_block_str);

    storage_manager.delete_data(1)?;

    storage_manager.close_session();
    storage_manager.start_write_session()?;
    storage_manager.write_data("this is a third test".as_bytes().to_vec(), None)?;

    return Ok(());
}
