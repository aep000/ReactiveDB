use crate::types::DataType;
use serde::{Deserialize, Serialize};
use serde_yaml::Result;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DbConfig {
    pub tables: Vec<TableConfig>,
    pub storage_destination: String,
    pub action_config: Option<Importable<ActionEnvConfig>>
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TableConfig {
    Derived(TransformTableConfig),
    Source(SourceTableConfig),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceTableConfig {
    pub name: String,
    pub columns: BTreeMap<String, DataType>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransformTableConfig {
    pub name: String,
    pub transform_definition: TransformType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TransformType {
    FunctionTransform(FunctionTransformConfig),
    FilterTransform(FilterTransformConfig),
    UnionTransform(UnionTransformConfig),
    AggregationTransform(AggregationTransformConfig),
    ActionTransform(ActionTransformConfig)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionTransformConfig {
    pub source_table: String,
    pub functions: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnionTransformConfig {
    pub tables_and_foreign_keys: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FilterTransformConfig {
    pub source_table: String,
    pub filter: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AggregationTransformConfig {
    pub source_table: String,
    pub aggregated_column: String,
    pub functions: Vec<String>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActionEnvConfig {
    pub workspace_dir: String,
    pub actions: Vec<ActionConfig>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActionConfig {
    pub name: String,
    pub file: String,
    pub function: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActionTransformConfig {
    pub name: String,
    pub source_table: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Importable<T> {
    Import(String),
    Inline(T)
}

// TODO generalize this function for importables
pub fn read_config_file(file_path: String) -> io::Result<DbConfig> {
    let file = OpenOptions::new().read(true).open(&file_path)?;
    let config: Result<DbConfig> = serde_yaml::from_reader(file);
    match config {
        Ok(mut config) => {
            let action_config = match config.clone().action_config {
                Some(importable_action_conf) => {
                    match importable_action_conf {
                        Importable::Import(action_conf_path) => {
                            let file = OpenOptions::new().read(true).open(&action_conf_path)?;
                            let config: Result<ActionEnvConfig> = serde_yaml::from_reader(file);
                            Some(Importable::Inline(config.map_err(|e| {io::Error::new(io::ErrorKind::Other, format!("{:?}", e))})?))
                        }
                        Importable::Inline(action_cfg) => Some(Importable::Inline(action_cfg))
                    }
                }
                None => None
            };
            config.action_config = action_config;
            Ok(config)
        },
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e))),
    }
}
