use crate::constants::SOURCE_ENTRY_ID;
use crate::constants::ROW_ID_COLUMN_NAME;
use crate::database::Database;
use crate::config::parser::{ExpressionValue, Statement};
use crate::EntryValue;
use crate::Entry;
use crate::Expression;
use std::collections::BTreeMap;

// Transform struct generated after parsing the config file
#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
pub enum Transform {
    Filter(Statement),
    Union(Vec<(String, String)>),
    Function(Vec<Statement>),
    //TODO Impl Aggregate
    Aggregate,
}

impl Transform {
    pub fn execute(
        &self,
        transaction: Entry,
        table_name: &String,
        db: &Database,
    ) -> Option<Entry> {
        match self {
            Transform::Function(statments) => {
                match Transform::function_transform(statments, transaction) {
                    Ok(entry) => Some(entry),
                    Err(something) => {
                        print!("{}: {}", table_name, something);
                        return None;
                    }
                }
            }
            Transform::Filter(statement) => {
                match Transform::filter_transform(statement, transaction) {
                    Ok(entry) => entry,
                    Err(something) => {
                        print!("{}: {}", table_name, something);
                        return None;
                    }
                }
            }
            //TODO impliment Union and Aggregate
            _ => None,
        }
    }

    fn function_transform(
        statements: &Vec<Statement>,
        transaction: Entry,
    ) -> std::result::Result<Entry, String> {
        let mut map: Entry = BTreeMap::new();
        let source_uuid = transaction.get(&ROW_ID_COLUMN_NAME.to_string()).unwrap();
        map.insert(SOURCE_ENTRY_ID.to_string(), source_uuid.clone());
        for statement in statements {
            let result = match statement {
                Statement::Assignment(dest, expr) => {
                    Some((dest, execute_expression(&transaction, expr)?))
                }
                _ => None,
            };
            let _ = match result {
                Some((dest, res)) => map.insert(dest.to_string(), res),
                None => None,
            };
        }
        return Ok(map);
    }

    fn filter_transform(
        statement: &Statement,
        mut transaction: Entry,
    ) -> std::result::Result<Option<Entry>, String> {
        match statement {
            Statement::Comparison(expr) => {
                let result = execute_expression(&transaction, expr)?;
                match result {
                    EntryValue::Bool(b) => {
                        if b {
                            let source_uuid = transaction.get(&ROW_ID_COLUMN_NAME.to_string()).unwrap().clone();
                            transaction.insert(SOURCE_ENTRY_ID.to_string(), source_uuid);
                            return Ok(Some(transaction));
                        }
                        return Ok(None);
                    }
                    _ => Err("Filter statement must result in boolean".to_string()),
                }
            }
            _ => Err("Assignment statement not allowed in filter".to_string()),
        }
    }

    fn union_transform(
        table_foreign_key_pairs: &Vec<(String, String)>,
        transaction: Entry,
        table_name: String,
        db: &mut Database,
    ) -> std::result::Result<Entry, String> {
        let dest_table = db.tables.get_mut(&table_name);
        let mut foreign_key = "".to_string();
        // This is slow and should be solved
        for maybe_t in table_foreign_key_pairs {
            if maybe_t.0.eq(&table_name) {
                foreign_key = maybe_t.1.to_string();
                break;
            }
        }
        match dest_table {
            Some(table) => {
                let search_value = match transaction.get(&foreign_key) {
                    Some(v) => Ok(v),
                    None => Err(format!("Transaction missing key {}", table_name)),
                }?;
                let existing_entry_result = table.find_one(foreign_key, search_value);
                match existing_entry_result {
                    Ok(existing_entry_exists) => match existing_entry_exists {
                        Some(mut existing_entry) => {
                            for (k, v) in transaction {
                                existing_entry.insert(k, v);
                            }
                            return Ok(existing_entry);
                        }
                        None => {
                            return Ok(transaction);
                        }
                    },
                    Err(e) => Err(format!("{:?}", e)),
                }
            }
            None => Err(format!("No table of name {}", table_name)),
        }
    }
}

fn execute_expression(
    transaction: &Entry,
    expression: &Expression,
) -> std::result::Result<EntryValue, String> {
    return match expression {
        Expression::Operation(left, operation, right) => {
            let resolved_left = resolve_expression_value(transaction, left)?;
            let resolved_right = resolve_expression_value(transaction, right)?;
            return operation.evaluate(resolved_left, resolved_right);
        }
        _ => Err("Function expressions are currently unimplimented".to_string()),
    };
}

fn resolve_expression_value(
    transaction: &Entry,
    value: &ExpressionValue,
) -> std::result::Result<EntryValue, String> {
    return match value {
        ExpressionValue::Value(value) => Ok(value.clone()),
        ExpressionValue::TableReference(reference) => match transaction.get(reference) {
            Some(value) => Ok(value.clone()),
            None => Err(format!("Unable to find column matching: \"{}\"", reference)),
        },
        ExpressionValue::SubExpression(exp) => execute_expression(transaction, &exp),
    };
}
