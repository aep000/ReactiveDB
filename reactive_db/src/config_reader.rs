use std::fs::OpenOptions;
use crate::types::DataType;
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use std::io;
use serde_yaml::Result;

#[derive(Serialize, Deserialize, Debug)]
pub struct DbConfig {
    tables: Vec<TableConfig>,
    storage_destination: String
}
#[derive(Serialize, Deserialize, Debug)]
pub enum TableConfig {
    Derived(TransformTableConfig),
    Source(SourceTableConfig)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SourceTableConfig {
    name: String,
    columns: BTreeMap<String, DataType>

}
#[derive(Serialize, Deserialize, Debug)]
pub struct TransformTableConfig {
    source_table: String,
    transform_definition: TransformType
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TransformType {
    FunctionTransform(FunctionTransformConfig),
    FilterTransform,
    UnionTransform,
    AggregationTransform
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionTransformConfig {
    functions: Vec<String>
}

pub fn read_config_file(file_path: String) -> io::Result<DbConfig> {
    let file = OpenOptions::new().read(true).open(&file_path)?;
    let config: Result<DbConfig> = serde_yaml::from_reader(file);
    match config {
        Ok(config) => Ok(config),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))
    }
}