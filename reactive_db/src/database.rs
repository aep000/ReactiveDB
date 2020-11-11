


use crate::parser::ExpressionValue;
use std::collections::BTreeMap;
use crate::EntryValue;
use crate::Expression;
use std::collections::HashMap;
use crate::Table;
use crate::config_reader::{DbConfig, TableConfig};

pub struct Database {
    pub tables: HashMap<String, Table>
}


impl Database {
    // TODO
    fn from_config(config: DbConfig) -> Database {
        let tables:HashMap<String, Table> = HashMap::new();
        for table in config.tables {
            match table {
                TableConfig::Source(source_config) => {
                    
                }
                _ => {}
            }
        }

        return Database {
            tables: HashMap::new()
        }
    }
}