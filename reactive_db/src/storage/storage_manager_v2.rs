use crate::{utilities::max_size_hash_map::MaxSizeHashMap};
use crate::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::cmp;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::io::{Error, ErrorKind};
use bzip2::Compression;
use bzip2::read::{BzEncoder, BzDecoder};

use super::storage_engine::StorageEngine;

const CACHE_SIZE: usize = 100;
const DATA_BLOCK_SIZE: u32 = 100;
const REFERENCE_BLOCK_SIZE: u32 = 4;
const SIZE_BLOCK_SIZE: u32 = 4;
const TOTAL_BLOCK_SIZE: u32 = DATA_BLOCK_SIZE + REFERENCE_BLOCK_SIZE+SIZE_BLOCK_SIZE;

pub struct StorageManagerV2 {
    pub file_name: String,
    pub open_blocks: BinaryHeap<isize>,
    pub closed_blocks: HashSet<u32>,
    pub number_of_blocks: u32,
    pub session_open: bool,
    pub open_file: Option<File>,
    cache: MaxSizeHashMap<u32, Vec<u8>>,
    debug: bool
}

impl StorageEngine for StorageManagerV2 {
    fn start_read_session(&mut self) -> io::Result<()> {
        self.session_open = true;
        self.open_file = Some(OpenOptions::new().read(true).open(&self.file_name)?);
        return Ok(());
    }

    fn start_write_session(&mut self) -> io::Result<()> {
        self.session_open = true;
        self.open_file = Some(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.file_name)?,
        );
        return Ok(());
    }

    fn end_session(&mut self) {
        self.session_open = false;
        self.open_file = None;
    }

    fn allocate_block(&mut self) -> u32 {
        match self.open_blocks.pop() {
            Some(n) => {
                let block = (-1 * n) as u32;
                if self.closed_blocks.contains(&block) {
                    return self.allocate_block();
                }
                return block;
            }
            None => {
                self.number_of_blocks += 1;
                return self.number_of_blocks;
            }
        }
    }

    fn write_data(&mut self, data: Vec<u8>, starting_block: Option<u32>) -> io::Result<u32> {
        if !self.debug {
            let mut compressed = vec![];
            BzEncoder::new(data.as_slice(), Compression::Default)
                .read_to_end(&mut compressed)
                .unwrap();
            let data = compressed;
        }
        self.start_write_session()?;
        let root_block: u32 = match starting_block {
            Some(n) => {
                if self.number_of_blocks <= n {
                    self.number_of_blocks += 1;
                }
                self.closed_blocks.insert(n);
                n
            }
            None => self.allocate_block(),
        };
        let mut cursor = 0;
        let mut size_remaining = data.len();
        let mut current_block = root_block;
        while cursor < data.len() {
            let end = cursor + (DATA_BLOCK_SIZE as usize);
            let mut next_ref = 0;
            let mut next_ref_to_write = vec![0; REFERENCE_BLOCK_SIZE as usize];
            let mut endian_rep_size = vec![];
            endian_rep_size.write_u32::<BigEndian>(size_remaining as u32)?;
            let mut size_remaining_to_write = vec![0; SIZE_BLOCK_SIZE as usize - endian_rep_size.len()];
            size_remaining_to_write.extend(endian_rep_size);
            if end < data.len() {
                let mut endian_rep = vec![];
                next_ref = self.allocate_block();
                endian_rep.write_u32::<BigEndian>(next_ref)?;
                next_ref_to_write = vec![0; REFERENCE_BLOCK_SIZE as usize - endian_rep.len()];
                next_ref_to_write.extend(endian_rep);
                size_remaining -= DATA_BLOCK_SIZE as usize;
            }
            let mut to_write = data[cursor..cmp::min(end, data.len())].to_vec();
            let additional_spacing = vec![0; (DATA_BLOCK_SIZE as usize) - to_write.len()];
            to_write.extend(additional_spacing);
            to_write.extend(next_ref_to_write);
            to_write.extend(size_remaining_to_write);
            self.write_block(current_block, to_write)?;
            cursor = end;
            current_block = next_ref;
        }
        return Ok(root_block);
    }

    fn read_data(&mut self, starting_block: u32) -> io::Result<Vec<u8>> {
        let mut block_to_read: usize = starting_block as usize;
        let mut output = vec![];
        while block_to_read != 0 {
            let raw_block = self.read_block(block_to_read as u32)?;
            if raw_block.len() <= DATA_BLOCK_SIZE as usize {
                Err(Error::new(ErrorKind::Other, "Error datablock too small"))?;
            }
            let mut data_block = raw_block[..DATA_BLOCK_SIZE as usize].to_vec();
            let next_block_raw = raw_block[(DATA_BLOCK_SIZE) as usize..].to_vec();
            let next_ref_block = next_block_raw[..REFERENCE_BLOCK_SIZE as usize].to_vec();
            let size_of_data_block = next_block_raw[REFERENCE_BLOCK_SIZE as usize..].to_vec();
            block_to_read = Cursor::new(next_ref_block).read_u32::<BigEndian>().unwrap() as usize;
            let size_of_data = Cursor::new(size_of_data_block).read_u32::<BigEndian>().unwrap() as usize;
            if size_of_data < DATA_BLOCK_SIZE as usize {
                data_block = data_block[..size_of_data].to_vec();
            }
            output.extend(data_block);
        }

        if output == vec![0; output.len()] {
            return Ok(output);
        }
        if !self.debug {
            let mut decompressed = vec![];
            //println!("{:?}", output);
            BzDecoder::new(output.as_slice())
                .read_to_end(&mut decompressed)
                .unwrap();
            return Ok(decompressed);
        }
        return Ok(output);
    }

    fn delete_data(&mut self, starting_block: u32) -> io::Result<()> {
        let mut block_to_read: usize = starting_block as usize;
        while block_to_read != 0 {
            let raw_block = self.read_block(block_to_read as u32)?;
            let next_block_raw = raw_block[(DATA_BLOCK_SIZE) as usize..].to_vec();
            self.delete_block(block_to_read as u32)?;
            if block_to_read != 1 && block_to_read != 0 {
                self.open_blocks.push(-1 * block_to_read as isize);
                self.closed_blocks.remove(&(block_to_read as u32));
            }
            block_to_read = Cursor::new(next_block_raw).read_u32::<BigEndian>().unwrap() as usize;
        }
        return Ok(());
    }

    fn is_empty(&mut self, block: u32) -> io::Result<bool> {
        if block > self.number_of_blocks {
            return Ok(true);
        }
        let block = self.read_block(block)?;
        return Ok(block == vec![0; TOTAL_BLOCK_SIZE as usize]);
    }

    fn get_file_name(&mut self) -> String {
        self.file_name.clone()
    }
}

impl StorageManagerV2 {
    pub fn new(file_name: String) -> io::Result<StorageManagerV2> {
        let mut manager = StorageManagerV2 {
            file_name: file_name,
            open_blocks: BinaryHeap::new(),
            closed_blocks: HashSet::new(),
            number_of_blocks: 0,
            session_open: false,
            open_file: None,
            cache: MaxSizeHashMap::new(CACHE_SIZE),
            debug: true
        };
        manager.start_write_session()?;
        manager.update_open_blocks()?;
        manager.end_session();

        return Ok(manager);
    }

    pub fn is_v2_storage_manager(file_name: String) -> io::Result<bool> {
        /*let mut manager = StorageManagerV2 {
            file_name: file_name,
            open_blocks: BinaryHeap::new(),
            closed_blocks: HashSet::new(),
            number_of_blocks: 0,
            session_open: false,
            open_file: None,
            cache: MaxSizeHashMap::new(CACHE_SIZE)
        };
        manager.start_read_session()?;
        let data = manager.read_block(0);
        match data {
            Err(_) => Ok(false),
            Ok(d) => {
                if d == vec![2] {
                    return Ok(true);
                }
                Ok(false)
            }
        }*/
        return Ok(true);
    }

    // Write to a specific block
    fn write_block(&mut self, block_number: u32, mut data: Vec<u8>) -> io::Result<()> {
        if !self.session_open {
            return Err(Error::new(ErrorKind::Other, "Session not open"));
        }
        let mut file = self.open_file.as_ref().unwrap();
        file.seek(SeekFrom::Start((block_number * TOTAL_BLOCK_SIZE) as u64))?;
        let mut writer = BufWriter::new(file);
        let to_write: Vec<u8> = vec![0; TOTAL_BLOCK_SIZE as usize - data.len()];
        data.extend(to_write);
        self.cache.insert(block_number, data.clone());
        writer.write(&data)?;
        writer.flush()?;
        return Ok(());
    }
    // Read a specific block
    fn read_block(&mut self, block_number: u32) -> io::Result<Vec<u8>> {
        if !self.session_open {
            return Err(Error::new(ErrorKind::Other, "Session not open"));
        }
        let is_in_cache = match self.cache.get(&block_number) {
            Some(block) => {
                return Ok(block.clone())
            },
            None => false
        };
        
        let mut file = self.open_file.as_ref().unwrap();
        file.seek(SeekFrom::Start((block_number * TOTAL_BLOCK_SIZE) as u64))?;

        let mut reader = BufReader::with_capacity(TOTAL_BLOCK_SIZE as usize, file);
        let buffer = reader.fill_buf()?;
        if !is_in_cache {
            self.cache.insert(block_number, buffer.to_vec());
        }
        return Ok(buffer.to_vec());
    }

    //Delete specific block
    fn delete_block(&mut self, block_number: u32) -> io::Result<()> {
        if !self.session_open {
            return Err(Error::new(ErrorKind::Other, "Session not open"));
        }
        self.cache.remove(block_number);
        let mut file = self.open_file.as_ref().unwrap();
        file.seek(SeekFrom::Start((block_number * TOTAL_BLOCK_SIZE) as u64))?;
        let fill = vec![0; TOTAL_BLOCK_SIZE as usize];
        self.write_block(block_number, fill)?;
        return Ok(());
    }

    fn update_open_blocks(&mut self) -> io::Result<()> {
        let file = self.open_file.as_ref().unwrap();
        let mut open_blocks = vec![];
        let empty_block: Vec<u8> = vec![0; TOTAL_BLOCK_SIZE as usize];
        let file_len = file.metadata()?.len();
        let num_blocks = file_len / (TOTAL_BLOCK_SIZE as u64);
        for n in 2..num_blocks {
            let block = self.read_block(n as u32)?;
            if block == empty_block {
                open_blocks.push(n as u32);
            }
        }
        for block in open_blocks {
            self.open_blocks.push(-1 * block as isize);
        }
        self.number_of_blocks = num_blocks as u32;
        return Ok(());
    }
}
