
use std::io::{Error, ErrorKind};
use std::fs::File;
use crate::io::Cursor;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::prelude::*;
use std::io;
use std::io::SeekFrom;
use std::collections::BinaryHeap;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::cmp;

const DATA_BLOCK_SIZE: u32 = 5;
const REFERENCE_BLOCK_SIZE: u32 = 4;
const TOTAL_BLOCK_SIZE: u32 = DATA_BLOCK_SIZE + REFERENCE_BLOCK_SIZE;

pub struct StorageManager {
    pub file_name: String,
    pub open_blocks: BinaryHeap<isize>,
    pub number_of_blocks: u32,
    pub session_open: bool,
    pub open_file: Option<File>
}

impl StorageManager{

    pub fn start_read_session(&mut self) -> io::Result<()>{
        self.session_open = true;
        self.open_file = Some(OpenOptions::new().read(true).open(&self.file_name)?);
        return Ok(());
    }

    pub fn start_write_session(&mut self) -> io::Result<()>{
        self.session_open = true;
        self.open_file = Some(OpenOptions::new().read(true).write(true).open(&self.file_name)?);
        return Ok(());
    }

    pub fn close_session(&mut self){
        self.session_open = false;
        self.open_file = None;
    }

    pub fn write_block(&mut self, block_number: u32, data: Vec<u8>) -> io::Result<()> {
        if !self.session_open {
            return Err(Error::new(ErrorKind::Other, "Session not open"))
        }
        let mut file = self.open_file.as_ref().unwrap();
        file.seek(SeekFrom::Start((block_number*TOTAL_BLOCK_SIZE) as u64))?;
        let mut writer = BufWriter::new(file);
        let mut to_write: Vec<u8> = vec![0; TOTAL_BLOCK_SIZE as usize - data.len()];
        to_write.extend(data);
        writer.write(&to_write)?;
        writer.flush()?;
        return Ok(());
    }

    pub fn read_block(&mut self, block_number: u32,) -> io::Result<Vec<u8>>{
        if !self.session_open {
            return Err(Error::new(ErrorKind::Other, "Session not open"))
        }
        let mut file = self.open_file.as_ref().unwrap();
        file.seek(SeekFrom::Start((block_number*TOTAL_BLOCK_SIZE) as u64))?;

        let mut reader = BufReader::with_capacity(TOTAL_BLOCK_SIZE as usize, file);
        let buffer = reader.fill_buf()?;

        return Ok(buffer.to_vec());
    }

    pub fn delete_block(&mut self, block_number: u32) -> io::Result<()>{
        if !self.session_open {
            return Err(Error::new(ErrorKind::Other, "Session not open"))
        }
        let mut file = self.open_file.as_ref().unwrap();
        file.seek(SeekFrom::Start((block_number*TOTAL_BLOCK_SIZE) as u64))?;
        let fill = vec![0; TOTAL_BLOCK_SIZE as usize];
        self.write_block(block_number, fill)?;
        return Ok(())
    }

    pub fn get_block(&mut self) -> u32{
        match self.open_blocks.pop() {
            Some(n) => (-1*n) as u32,
            None => {
                self.number_of_blocks += 1;
                return self.number_of_blocks
            }
        }
    }

    pub fn write_data(&mut self, data: Vec<u8>, starting_block: Option<u32>,) -> io::Result<()>{
        let root_block: u32 = match starting_block {
            Some(n) => n,
            None => self.get_block()
        };
        let mut cursor = 0;
        let mut current_block = root_block;
        while cursor < data.len(){
            let end = cursor+(DATA_BLOCK_SIZE as usize);
            let mut next_ref = 0;
            let mut next_ref_to_write = vec![0; REFERENCE_BLOCK_SIZE as usize];
            if end < data.len() {
                let mut endian_rep = vec![];
                next_ref = self.get_block();
                endian_rep.write_u32::<BigEndian>(next_ref)?;
                next_ref_to_write = vec![0; REFERENCE_BLOCK_SIZE as usize - endian_rep.len()];
                next_ref_to_write.extend(endian_rep);
            };
            let mut to_write = data[cursor..cmp::min(end,data.len())].to_vec();
            to_write.extend(next_ref_to_write);
            self.write_block(current_block, to_write)?;
            cursor = end;
            current_block = next_ref;
        }
        return Ok(());
    }

    pub fn read_data(&mut self, starting_block:u32) -> io::Result<Vec<u8>>{
        let mut block_to_read: usize = starting_block as usize;
        let mut output = vec![];
        while block_to_read != 0 {
            let raw_block = self.read_block(block_to_read as u32)?;
            let data_block = &raw_block[..DATA_BLOCK_SIZE as usize].to_vec();
            output.extend(data_block);
            let next_block_raw = raw_block[(DATA_BLOCK_SIZE) as usize ..].to_vec();
            block_to_read = Cursor::new(next_block_raw).read_u32::<BigEndian>().unwrap() as usize;
            
        }
        return Ok(output)
    }

    pub fn delete_data(&mut self, starting_block:u32) -> io::Result<()>{
        let mut block_to_read: usize = starting_block as usize;
        while block_to_read != 0 {
            let raw_block = self.read_block(block_to_read as u32)?;
            let next_block_raw = raw_block[(DATA_BLOCK_SIZE) as usize ..].to_vec();
            self.delete_block(block_to_read as u32)?;
            self.open_blocks.push(-1*block_to_read as isize);
            block_to_read = Cursor::new(next_block_raw).read_u32::<BigEndian>().unwrap() as usize;
        }
        return Ok(())
    }
}
