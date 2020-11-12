


use crate::parser::ExpressionValue;
use std::collections::BTreeMap;
use crate::EntryValue;
use crate::Expression;
use std::collections::HashMap;
use crate::Table;
use crate::config_reader::{DbConfig, TableConfig, TransformTableConfig, TransformType};

pub struct Database {
    pub tables: HashMap<String, Table>
}


impl Database {
    // TODO
    pub fn from_config(config: DbConfig) -> Database {
        let tables:HashMap<String, Table> = HashMap::new();
        for table in config.tables {
            match table {
                TableConfig::Source(source_config) => {
                    
                },
                TableConfig::Derived(config) => parse_transform_config(config),
                _ => {}
            }
        }

        return Database {
            tables: HashMap::new()
        }
    }
}

fn parse_transform_config(config: TransformTableConfig) {
    let name = config.name;
    let columns = vec![];
    let transform = match config.transform_definition {
        TransformType::FunctionTransform(fn_transform) => {
            let mut statements = vec![];
            // TODO finish this
        }
    };

}