#[cfg(test)]
mod tests {
    
    use std::fs;

    use crate::storage::{storage_engine::StorageEngine, storage_manager_v2::StorageManagerV2};
    fn get_storage_manager(file_name: String) -> StorageManagerV2 {
        let _ = fs::remove_file(file_name.clone());
        return StorageManagerV2::new(file_name).unwrap();
    }

    #[test]
    fn write_small_data() {
        let mut storage_manager = get_storage_manager("test_results/storage_manager_test1.db".to_string());
        let test_value = "abcd";
        storage_manager.start_write_session().unwrap();
        let loc = storage_manager.write_data(test_value.as_bytes().to_vec(), None).unwrap();
        let recieved_data = storage_manager.read_data(loc).unwrap();
        let new_data = std::str::from_utf8(&recieved_data).unwrap();
        assert_eq!(test_value, new_data);
    }

    #[test]
    fn write_large_data() {
        let mut storage_manager = get_storage_manager("test_results/storage_manager_test1.db".to_string());
        let test_value = "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        storage_manager.start_write_session().unwrap();
        let loc = storage_manager.write_data(test_value.as_bytes().to_vec(), None).unwrap();
        let recieved_data = storage_manager.read_data(loc).unwrap();
        let new_data = std::str::from_utf8(&recieved_data).unwrap();
        assert_eq!(test_value, new_data);
    }

    #[test]
    fn write_exact_data_block_sized_data() {
        let mut storage_manager = get_storage_manager("test_results/storage_manager_test1.db".to_string());
        let test_value = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        storage_manager.start_write_session().unwrap();
        let loc = storage_manager.write_data(test_value.as_bytes().to_vec(), None).unwrap();
        let recieved_data = storage_manager.read_data(loc).unwrap();
        let new_data = std::str::from_utf8(&recieved_data).unwrap();
        assert_eq!(test_value, new_data);
    }

    #[test]
    fn write_multiple_different_pieces_of_data_data() {
        let mut storage_manager = get_storage_manager("test_results/storage_manager_test1.db".to_string());
        let test_value1 = "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        let test_value2 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
        storage_manager.start_write_session().unwrap();
        let loc1 = storage_manager.write_data(test_value1.as_bytes().to_vec(), None).unwrap();
        let loc2 = storage_manager.write_data(test_value2.as_bytes().to_vec(), None).unwrap();

        let recieved_data1 = storage_manager.read_data(loc1).unwrap();
        let new_data1 = std::str::from_utf8(&recieved_data1).unwrap();
        assert_eq!(test_value1, new_data1);

        let recieved_data2 = storage_manager.read_data(loc2).unwrap();
        let new_data2 = std::str::from_utf8(&recieved_data2).unwrap();
        assert_eq!(test_value2, new_data2);
    }
}