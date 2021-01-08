use std::io;

pub trait StorageEngine{
    fn start_read_session(&mut self) -> io::Result<()>;

    fn start_write_session(&mut self) -> io::Result<()>;

    fn end_session(&mut self);

    fn allocate_block(&mut self) -> u32;

    fn write_data(&mut self, data: Vec<u8>, starting_block: Option<u32>) -> io::Result<u32>;

    fn read_data(&mut self, starting_block: u32) -> io::Result<Vec<u8>>;

    fn delete_data(&mut self, starting_block: u32) -> io::Result<()>;

    fn is_empty(&mut self, block: u32) -> io::Result<bool>;

    fn get_file_name(&mut self) -> String;
}