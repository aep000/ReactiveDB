use crate::{config::expression_parser::{ExpressionValue, Statement}, constants::AGGREGATION_KEY, types::{DBEdit, EditType}};
use crate::constants::ROW_ID_COLUMN_NAME;
use crate::constants::SOURCE_ENTRY_ID;
use crate::constants::UNION_MATCHING_KEY;
use crate::database::Database;
use crate::Entry;
use crate::EntryValue;
use crate::Expression;
use std::collections::BTreeMap;

// Transform struct generated after parsing the config file
#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
pub enum Transform {
    Filter(Statement),
    Union(Vec<(String, String)>),
    Function(Vec<Statement>),
    //TODO Impl Aggregate
    Aggregate((Vec<Statement>, String)),
    None,
}

impl Transform {
    pub fn execute(
        &self,
        transaction: Entry,
        table_name: &String,
        db: &mut Database,
        source_table: Option<&String>,
    ) -> Option<DBEdit> {
        match self {
            Transform::Function(statments) => {
                match Transform::function_transform(statments, transaction) {
                    Ok(entry) => Some(DBEdit::new(table_name.clone(), EditType::Insert(entry))),
                    Err(something) => {
                        print!("{}: {}", table_name, something);
                        return None;
                    }
                }
            }
            Transform::Filter(statement) => {
                match Transform::filter_transform(statement, transaction) {
                    Ok(wrapped_entry) => match wrapped_entry {
                        Some(entry) => Some(DBEdit::new(table_name.clone(), EditType::Insert(entry))),
                        None => None
                    },
                    Err(something) => {
                        print!("{}: {}", table_name, something);
                        return None;
                    }
                }
            }

            Transform::Union(columns) => match Transform::union_transform(
                columns,
                transaction.clone(),
                table_name,
                source_table.unwrap(),
                db,
            ){
                Some(entry) => {
                    let mut foreign_value = None;
                    for (table, key) in columns {
                        if table == source_table.unwrap() {
                            foreign_value = match transaction.get(key) {
                                Some(val) => Some(val),
                                None => panic!("Foreign key column in transaction"),
                            };
                        }
                    }
                    //TODO maybe make some kindof unmatchable type so that this doesnt mess things up
                    return Some(DBEdit::new(
                        table_name.clone(),
                        EditType::Update(entry,
                            UNION_MATCHING_KEY.to_string(),
                            foreign_value.unwrap_or(&EntryValue::Str("Does Not Match Anything abcdefg".to_string())).clone()
                        )
                    ))
                },
                None => None
            },

            Transform::Aggregate((statements, aggregation_column)) => match Transform::aggregate_transform(
                statements, 
                transaction.clone(),
                table_name, 
                source_table.unwrap(), 
                aggregation_column, 
                db
            ){
                Ok(entry) => Some(
                    DBEdit::new(
                        table_name.clone(), 
                        EditType::Update(
                            entry, AGGREGATION_KEY.to_string(),
                            transaction.get(aggregation_column).unwrap().clone()
                        ))),
                Err(something) => {
                    print!("{}: {}", table_name, something);
                    return None;
                }
            },
            Transform::None => Some(DBEdit::new(table_name.clone(), EditType::Insert(transaction)))
            //_ => None,
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
                            let source_uuid = transaction
                                .get(&ROW_ID_COLUMN_NAME.to_string())
                                .unwrap()
                                .clone();
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
        table_name: &String,
        source_table: &String,
        db: &mut Database,
    ) -> Option<Entry> {
        let mut foreign_value = None;
        let mut foreign_key = None;
        for (table, key) in table_foreign_key_pairs {
            if table == source_table {
                foreign_value = match transaction.get(key) {
                    Some(val) => Some(val),
                    None => panic!("Foreign key column in transaction"),
                };
                foreign_key = Some(key);
            }
        }

        match db.get_all(
            table_name,
            UNION_MATCHING_KEY.to_string(),
            foreign_value.unwrap().clone(),
        ) {
            Ok(commited_edits) => {
                let old_entries: Vec<Entry> = commited_edits;
                if old_entries.len() > 0 {
                    let mut old_entry = old_entries[0].clone();
                    for (key, value) in transaction {
                        old_entry.insert(key, value);
                    }
                    Some(old_entry)
                } else {
                    let mut new_entry = transaction.clone();
                    new_entry.remove(foreign_key.unwrap());
                    new_entry.insert(
                        UNION_MATCHING_KEY.to_string(),
                        foreign_value.unwrap().clone(),
                    );
                    Some(new_entry)
                }
            }
            Err(_) => {
                let mut new_entry = transaction.clone();
                new_entry.remove(foreign_key.unwrap());
                new_entry.insert(
                    UNION_MATCHING_KEY.to_string(),
                    foreign_value.unwrap().clone(),
                );
                Some(new_entry)
            }
        }
    }
    fn aggregate_transform(
        statements: &Vec<Statement>,
        mut transaction: Entry,
        table_name: &String,
        source_table: &String,
        aggregation_column: &String,        
        db: &mut Database,
    ) -> std::result::Result<Entry, String> {
        let mut map: Entry = BTreeMap::new();
        let source_uuid = transaction.get(&ROW_ID_COLUMN_NAME.to_string()).unwrap();
        let mut source_transactions = db.get_all(source_table, aggregation_column.clone(), transaction.get(aggregation_column).unwrap().clone())?;
        let aggregation_key = transaction.get(aggregation_column).unwrap();
        map.insert(SOURCE_ENTRY_ID.to_string(), source_uuid.clone());
        map.insert(AGGREGATION_KEY.to_string(), aggregation_key.clone());
        let mut n = 0;
        for mut source_transaction in source_transactions.drain(..) {
            for statement in statements {
                let result = match statement {
                    Statement::Assignment(dest, expr) => {
                        let key = "memo.".to_owned() + dest;
                        if n == 0 {
                            transaction.insert(key.clone(), EntryValue::Integer(0));
                            let first = execute_expression(&transaction, expr)?;
                            source_transaction.insert(key.clone(), first.clone());
                        }
                        else{
                            let existing =  map.get(&dest.to_string()).unwrap_or_else(|| -> &EntryValue {&EntryValue::Integer(0)});
                            source_transaction.insert(key.clone(), existing.clone());
                        }
                        let step_result = execute_expression(&source_transaction, expr)?;
                        Some((dest, step_result))
                    }
                    _ => None,
                };
                let _ = match result {
                    Some((dest, res)) =>{
                        source_transaction.insert("memo.".to_owned() + dest, res.clone());
                        transaction.insert("memo.".to_owned()+ dest, res.clone());
                        map.insert(dest.to_string(), res)
                    },
                    None => None,
                };
            }
            n+=1;
        }
        return Ok(map);
    }
}

fn execute_expression(
    transaction: &Entry,
    expression: &Expression,
) -> std::result::Result<EntryValue, String> {
    return match expression {
        Expression::Operation(left, operation, right) => {
            //println!("\nLeft: {:?} Operation {:?} Right: {:?}\n", left, operation, right);
            //println!("Transaction: {:?}\n", transaction);
            let resolved_left = resolve_expression_value(transaction, left)?;
            let resolved_right = resolve_expression_value(transaction, right)?;
            return operation.evaluate(resolved_left, resolved_right);
        }
        Expression::Constant(value) => resolve_expression_value(transaction, value),
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
