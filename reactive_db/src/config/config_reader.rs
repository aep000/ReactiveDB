use crate::types::DataType;
use serde::{Deserialize, Serialize};
use serde_yaml::Result;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;

#[derive(Serialize, Deserialize, Debug)]
pub struct DbConfig {
    pub tables: Vec<TableConfig>,
    pub storage_destination: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub enum TableConfig {
    Derived(TransformTableConfig),
    Source(SourceTableConfig),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SourceTableConfig {
    pub name: String,
    pub columns: BTreeMap<String, DataType>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct TransformTableConfig {
    pub name: String,
    pub transform_definition: TransformType,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TransformType {
    FunctionTransform(FunctionTransformConfig),
    FilterTransform(FilterTransformConfig),
    UnionTransform(UnionTransformConfig),
    // TODO impliment Aggregations
    AggregationTransform,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionTransformConfig {
    pub source_table: String,
    pub functions: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UnionTransformConfig {
    pub tables_and_foreign_keys: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FilterTransformConfig {
    pub soure_table: String,
    pub filter: String,
}

pub fn read_config_file(file_path: String) -> io::Result<DbConfig> {
    let file = OpenOptions::new().read(true).open(&file_path)?;
    let config: Result<DbConfig> = serde_yaml::from_reader(file);
    match config {
        Ok(config) => Ok(config),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e))),
    }
}
