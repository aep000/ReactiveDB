#[cfg(test)]
mod tests {
    use crate::{config::config_reader::DbConfig, database_manager::DatabaseManager};
    use crate::read_config_file;
    use crate::Entry;
    use crate::EntryValue;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::fs;
    fn get_db(data_destination: String) -> DatabaseManager {
        let _ = fs::remove_dir_all(data_destination.clone());
        let _ = fs::create_dir(data_destination.clone());
        let config:DbConfig = read_config_file("test_cfg.yaml".to_string()).unwrap();
        
        DatabaseManager::from_config(config, data_destination.clone()).unwrap()
    }

    #[test]
    fn insert_many_fetch_one() {
        let mut dbm = get_db("db/test1".to_string());
        let arr = 0..29;
        let mut rng = rand::thread_rng();
        let mut middle_entry = None;
        let mut entries = vec![];
        for n in arr {
            let mut entry_to_insert = EntryBuilder::new();
            let i = rng.gen_range(0.0, 10.0) as isize;
            entry_to_insert.column("testForIteration", EntryValue::Integer(n));
            entry_to_insert.column("testForIndex", EntryValue::Integer(i));
            if n == 15 {
                middle_entry = Some(entry_to_insert.build());
            }
            if n < 5 {
                entries.push(entry_to_insert.build());
            }
            let (temp_dbm, results) = dbm.insert_entry(
                &"testTable".to_string(),
                entry_to_insert.build(),
                None);
            results.unwrap();
            dbm = temp_dbm;
        }
        // Test source
        let results = dbm
            .find_one(
                &"testTable".to_string(),
                "testForIteration".to_string(),
                EntryValue::Integer(15),
            )
            .unwrap()
            .unwrap();
        let unwrapped_insert = middle_entry.unwrap();
        assert_eq!(
            results.get("testForIteration").unwrap(),
            unwrapped_insert.get("testForIteration").unwrap()
        );
        assert_eq!(
            results.get("testForIndex").unwrap(),
            unwrapped_insert.get("testForIndex").unwrap()
        );
        print!("{:?}", results.get("_entryId").unwrap());
        // Test derived
        let results = dbm
            .find_one(
                &"derived".to_string(),
                "_sourceEntryId".to_string(),
                results.get("_entryId").unwrap().clone(),
            )
            .unwrap()
            .unwrap();
        let iteration = unwrapped_insert.get("testForIteration").unwrap();
        match iteration {
            EntryValue::Integer(n) => assert_eq!(
                results.get("newColumn").unwrap(),
                &EntryValue::Integer(n + 2)
            ),
            _ => panic!("Inserted value is not an integer as expected"),
        }

        let results = dbm
            .less_than_search(
                &"testTable".to_string(),
                "testForIteration".to_string(),
                EntryValue::Integer(5),
            )
            .unwrap();
        for n in 0..results.len() {
            assert_eq!(
                results[n].get("testForIteration").unwrap(),
                entries[n].get("testForIteration").unwrap()
            );
            assert_eq!(
                results[n].get("testForIndex").unwrap(),
                entries[n].get("testForIndex").unwrap()
            );
        }
    }

    #[test]
    fn insert_many_less_than() {
        let mut dbm = get_db("db/test2".to_string());
        let arr = 0..29;
        let mut rng = rand::thread_rng();
        let mut entries = vec![];
        for n in arr {
            let mut entry_to_insert = EntryBuilder::new();
            let i = rng.gen_range(0.0, 10.0) as isize;
            entry_to_insert.column("testForIteration", EntryValue::Integer(n));
            entry_to_insert.column("testForIndex", EntryValue::Integer(i));
            if n < 5 {
                entries.push(entry_to_insert.build());
            }
            let (temp_dbm, results) = dbm.insert_entry(&"testTable".to_string(), entry_to_insert.build(), None);
            results.unwrap();
            dbm = temp_dbm;
        }
        // Test source
        let results = dbm
            .less_than_search(
                &"testTable".to_string(),
                "testForIteration".to_string(),
                EntryValue::Integer(5),
            )
            .unwrap();
        for n in 0..results.len() {
            assert_eq!(
                results[n].get("testForIteration").unwrap(),
                entries[n].get("testForIteration").unwrap()
            );
            assert_eq!(
                results[n].get("testForIndex").unwrap(),
                entries[n].get("testForIndex").unwrap()
            );
        }
    }

    #[test]
    fn insert_many_greater_than() {
        let mut dbm = get_db("db/test3".to_string());
        let arr = 0..29;
        let mut rng = rand::thread_rng();
        let mut entries = vec![];
        for n in arr {
            let mut entry_to_insert = EntryBuilder::new();
            let i = rng.gen_range(0.0, 10.0) as isize;
            entry_to_insert.column("testForIteration", EntryValue::Integer(n));
            entry_to_insert.column("testForIndex", EntryValue::Integer(i));
            if n >= 10 {
                entries.push(entry_to_insert.build());
            }
            let (temp_dbm, result) = dbm.insert_entry(&"testTable".to_string(), entry_to_insert.build(), None);
            dbm = temp_dbm;
            result.unwrap();
        }
        // Test source
        let results = dbm
            .greater_than_search(
                &"testTable".to_string(),
                "testForIteration".to_string(),
                EntryValue::Integer(10),
            )
            .unwrap();
        for n in 0..results.len() {
            assert_eq!(
                results[n].get("testForIteration").unwrap(),
                entries[n].get("testForIteration").unwrap()
            );
            assert_eq!(
                results[n].get("testForIndex").unwrap(),
                entries[n].get("testForIndex").unwrap()
            );
        }
    }

    #[derive(Clone)]
    pub struct EntryBuilder {
        map: Entry,
    }
    impl EntryBuilder {
        pub fn new() -> EntryBuilder {
            return EntryBuilder {
                map: BTreeMap::new(),
            };
        }
        pub fn column(&mut self, key: &str, value: EntryValue) -> EntryBuilder {
            self.map.insert(key.to_string(), value);
            return self.clone();
        }
        pub fn build(&mut self) -> Entry {
            self.map.clone()
        }
    }
}
