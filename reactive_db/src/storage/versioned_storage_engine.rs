use std::path::Path;

use std::io;

use super::{storage_engine::StorageEngine, storage_manager::StorageManager, storage_manager_v2::StorageManagerV2};

// Structure for detecting storage format version changes
pub struct VersionedStorageEngine {
    storage_manager: StorageManagerVersion,
    file_name: String
}

pub enum StorageManagerVersion {
    V1(StorageManager),
    V2(StorageManagerV2)
}

impl StorageEngine for VersionedStorageEngine {
    fn start_read_session(&mut self) -> std::io::Result<()> {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.start_read_session(),
            StorageManagerVersion::V2(sm) => sm.start_read_session()
        }
    }

    fn start_write_session(&mut self) -> std::io::Result<()> {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.start_write_session(),
            StorageManagerVersion::V2(sm) => sm.start_write_session()
        }
    }

    fn end_session(&mut self) {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.end_session(),
            StorageManagerVersion::V2(sm) => sm.end_session()
        }
    }

    fn allocate_block(&mut self) -> u32 {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.allocate_block(),
            StorageManagerVersion::V2(sm) => sm.allocate_block()
        }
    }

    fn write_data(&mut self, data: Vec<u8>, starting_block: Option<u32>) -> std::io::Result<u32> {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.write_data(data, starting_block),
            StorageManagerVersion::V2(sm) => sm.write_data(data, starting_block)
        }
    }

    fn read_data(&mut self, starting_block: u32) -> std::io::Result<Vec<u8>> {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.read_data(starting_block),
            StorageManagerVersion::V2(sm) => sm.read_data(starting_block)
        }
    }

    fn delete_data(&mut self, starting_block: u32) -> std::io::Result<()> {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.delete_data(starting_block),
            StorageManagerVersion::V2(sm) => sm.delete_data(starting_block)
        }
    }

    fn is_empty(&mut self, block: u32) -> std::io::Result<bool> {
        let storage_manager_version = &mut self.storage_manager;
        match storage_manager_version {
            StorageManagerVersion::V1(sm) => sm.is_empty(block),
            StorageManagerVersion::V2(sm) => sm.is_empty(block)
        }
    }

    fn get_file_name(&mut self) -> String {
        self.file_name.clone()
    }
}

impl VersionedStorageEngine {
    // Run future migrations here
    pub fn new(file_name: String) -> io::Result<VersionedStorageEngine> {
        if Path::new(&file_name).exists() {
            if !StorageManagerV2::is_v2_storage_manager(file_name.clone())? {
                return Ok(VersionedStorageEngine {
                    file_name: file_name.clone(),
                    storage_manager: StorageManagerVersion::V1(StorageManager::new(file_name)?)
                })
            }
        }
        return Ok(VersionedStorageEngine {
            file_name: file_name.clone(),
            storage_manager: StorageManagerVersion::V2(StorageManagerV2::new(file_name)?)
        });
        
    }
}

