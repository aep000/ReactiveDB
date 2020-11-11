use crate::parser::ExpressionValue;
use crate::Expression;
use crate::EntryValue;
use crate::parser::Statement;
use std::collections::BTreeMap;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq,)]
pub enum Transform {
    Filter(Statement),
    Union(String, String),
    Function(Vec<Statement>),
    //TODO Impl Aggregate
    Aggregate
}


impl Transform {
    pub fn execute(self, transaction: BTreeMap<String, EntryValue>, table_name:String) -> Option<BTreeMap<String, EntryValue>>{
        match self {
            Transform::Function(statments) => {
                match Transform::function_transform(statments, transaction) {
                    Ok(entry) => Some(entry),
                    Err(something) => {
                        print!("{}: {}",table_name, something);
                        return None;
                    }
                }
            },
            Transform::Filter(statement) => {
                match Transform::filter_transform(statement, transaction) {
                    Ok(entry) => entry,
                    Err(something) => {
                        print!("{}: {}",table_name, something);
                        return None;
                    }
                }
            },
            //TODO impliment Union and Aggregate
            _ => None
        }
    }

    fn function_transform(statements: Vec<Statement>, transaction: BTreeMap<String, EntryValue>) -> std::result::Result<BTreeMap<String, EntryValue>, String> {
        let mut map: BTreeMap<String, EntryValue> = BTreeMap::new();
        for statement in statements {
            let result = match statement {
                Statement::Assignment(dest, expr) => Some((dest, execute_expression(&transaction, expr)?)),
                _ => None
            };
            let _ = match result {
                Some((dest, res)) => map.insert(dest, res),
                None => None
            };
        }
        return Ok(map);
    }

    fn filter_transform(statement: Statement, transaction: BTreeMap<String, EntryValue>) -> std::result::Result<Option<BTreeMap<String, EntryValue>>, String>{
        match statement {
            Statement::Comparison(expr) => {
                let result = execute_expression(&transaction, expr)?;
                match result {
                    EntryValue::Bool(b) => {
                        if b {
                            return Ok(Some(transaction));
                        }
                        return Ok(None);
                    }
                    _ => Err("Filter statement must result in boolean".to_string())
                }
            }
            _ => Err("Assignment statement not allowed in filter".to_string())
        }
    }
}

fn execute_expression(transaction: &BTreeMap<String, EntryValue>, expression: Expression) -> std::result::Result<EntryValue, String>{
    return match expression {
        Expression::Operation(left, operation, right) => {
            let resolved_left = resolve_expression_value(transaction, *left)?;
            let resolved_right = resolve_expression_value(transaction, *right)?;
            return operation.evaluate(resolved_left, resolved_right);
        }
        _ => Err("Function expressions are currently unimplimented".to_string())
    };
}

fn resolve_expression_value(transaction: &BTreeMap<String, EntryValue>, value: ExpressionValue)-> std::result::Result<EntryValue, String>{
    return match value {
        ExpressionValue::Value(value) => Ok(value),
        ExpressionValue::TableReference(reference) => {
            match transaction.get(&reference) {
                Some(value) => Ok(value.clone()),
                None => Err(format!("Unable to find column matching: \"{}\"", reference))
            }
        },
        ExpressionValue::SubExpression(exp) => execute_expression(transaction, exp)
    }
    
}