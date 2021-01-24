use std::collections::HashMap;

use crate::{actions::Action, types::DataType};
use crate::hooks::transforms::Transform;
use crate::table::{Column, Table, TableType};

use super::{config_reader::{TransformTableConfig, TransformType}, expression_parser::Statement};

pub fn parse_transform_config(
    config: TransformTableConfig,
    storage_path: String,
    actions: &HashMap<String, Action>
) -> Result<(Table, Transform), String> {
    let name = config.name;
    let mut columns = vec![];
    columns.push(Column::new("_entryId".to_string(), DataType::ID));
    let mut input_tables = vec![];
    let transform = match config.transform_definition {
        TransformType::FunctionTransform(config) => {
            columns.push(Column::new("_sourceEntryId".to_string(), DataType::ID));
            let mut statements = vec![];
            input_tables.push(config.source_table);
            for raw_statement in config.functions {
                statements.push(Statement::new_assignment(raw_statement)?);
            }
            Transform::Function(statements)
        }
        TransformType::FilterTransform(config) => {
            columns.push(Column::new("_sourceEntryId".to_string(), DataType::ID));
            let statement = Statement::new_comparison(config.filter)?;
            input_tables.push(config.source_table);
            Transform::Filter(statement)
        }
        TransformType::UnionTransform(config) => {
            for (table, _) in config.tables_and_foreign_keys.iter() {
                input_tables.push(table.clone());
            }
            Transform::Union(config.tables_and_foreign_keys)
        }
        TransformType::AggregationTransform(config) => {
            let mut statements = vec![];
            input_tables.push(config.source_table);
            for raw_statement in config.functions {
                statements.push(Statement::new_assignment(raw_statement)?);
            }
            Transform::Aggregate((statements, config.aggregated_column))
        }
        TransformType::ActionTransform(action_config) => {
            input_tables.push(action_config.source_table);
            let action = actions.get(&action_config.name);
            let action = match action {
                Some(action) => {
                    action.to_owned()
                }
                None => {panic!("Error: No action with name {} found", action_config.name)}
            };
            Transform::Action(action)
        }
    };
    let table = Table::new(name, columns, TableType::Derived(transform.clone()), storage_path);
    match table {
        Ok(mut t) => {
            t.input_tables = input_tables;
            Ok((t, transform))
        }
        Err(e) => Err(format!("{:?}", e)),
    }
}