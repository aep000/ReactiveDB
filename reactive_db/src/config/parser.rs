use crate::config::config_reader::{TransformTableConfig, TransformType};
use crate::table::TableType;
use crate::types::Comparison;
use crate::types::EntryValue;
use crate::types::Operation;
use crate::types::OperationOrComparison;
use crate::Column;
use crate::DataType;
use crate::Table;
use crate::Transform;

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Debug)]
pub enum Statement {
    Assignment(String, Expression),
    Comparison(Expression),
}

pub fn parse_transform_config(
    config: TransformTableConfig,
    storage_path: String,
) -> Result<Table, String> {
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
        _ => Err("Unsupported derived table".to_string())?,
    };
    let table = Table::new(name, columns, TableType::Derived(transform), storage_path);
    match table {
        Ok(mut t) => {
            t.input_tables = input_tables;
            Ok(t)
        }
        Err(e) => Err(format!("{:?}", e)),
    }
}

impl Statement {
    pub fn new_assignment(raw_assignment: String) -> Result<Statement, String> {
        let tokenized_assignment = lex_expression(&raw_assignment);
        for (i, t) in tokenized_assignment.iter().enumerate() {
            if t.eq(&Tokens::Assign) {
                if i == 1 {
                    let expression =
                        Expression::from_tokens(tokenized_assignment[i + 1..].to_vec())?;
                    let destination = match &tokenized_assignment[i - 1] {
                        Tokens::Word(destination) => Ok(destination),
                        _ => Err(format!(
                            "Assignment destination is not a word in statement {:?}",
                            raw_assignment
                        )),
                    }?;
                    return Ok(Statement::Assignment(destination.clone(), expression));
                } else if i == tokenized_assignment.len() - 2 {
                    let expression = Expression::from_tokens(tokenized_assignment[..i].to_vec())?;
                    let destination = match &tokenized_assignment[i + 1] {
                        Tokens::Word(destination) => Ok(destination),
                        _ => Err(format!(
                            "Assignment destination is not a word in statement {:?}",
                            raw_assignment
                        )),
                    }?;
                    return Ok(Statement::Assignment(destination.clone(), expression));
                } else {
                    return Err(format!(
                        "Error parsing assignment: Assignment found in middle of statement: {:?}",
                        raw_assignment
                    ));
                }
            }
        }
        return Err(format!(
            "Error parsing assignment: No assignment found in: {:?}",
            raw_assignment
        ));
    }
    pub fn new_comparison(raw_comparison: String) -> Result<Statement, String> {
        let tokenized_comparison = lex_expression(&raw_comparison);
        let expression = Expression::from_tokens(tokenized_comparison)?;
        return Ok(Statement::Comparison(expression));
    }
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Debug)]
pub enum Expression {
    FunctionCall(String, Box<ExpressionValue>),
    Operation(
        Box<ExpressionValue>,
        OperationOrComparison,
        Box<ExpressionValue>,
    ),
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum Tokens {
    Word(String),
    OpenParen,
    CloseParen,
    Number(String),
    Str(String),
    Assign,
    Operator(Operation),
    Comparison(Comparison),
}

impl Expression {
    pub fn from_tokens(tokens: Vec<Tokens>) -> Result<Expression, String> {
        let mut parens = 0;
        let mut output: Result<Expression, String> = Err("No Expression Found".to_string());
        for (i, t) in tokens.iter().enumerate() {
            match t {
                Tokens::OpenParen => parens += 1,
                Tokens::CloseParen => parens -= 1,
                Tokens::Comparison(comparison) => {
                    if parens == 0 {
                        let left_vec = tokens[..i].to_vec();
                        let right_vec = tokens[i + 1..].to_vec();
                        let left_expr_val = ExpressionValue::get_expression_value(left_vec)?;
                        let right_expr_val = ExpressionValue::get_expression_value(right_vec)?;
                        output = Ok(Expression::Operation(
                            Box::new(left_expr_val),
                            OperationOrComparison::Comparison(comparison.clone()),
                            Box::new(right_expr_val),
                        ));
                        break;
                    }
                }
                Tokens::Operator(operation) => {
                    if parens == 0 {
                        let left_vec = tokens[..i].to_vec();
                        let right_vec = tokens[i + 1..].to_vec();
                        let left_expr_val = ExpressionValue::get_expression_value(left_vec)?;
                        let right_expr_val = ExpressionValue::get_expression_value(right_vec)?;
                        output = Ok(Expression::Operation(
                            Box::new(left_expr_val),
                            OperationOrComparison::Operation(operation.clone()),
                            Box::new(right_expr_val),
                        ));
                    }
                }
                Tokens::Word(word) => {
                    if tokens.len() >= 3 && i < tokens.len() - 3 && parens == 0 {
                        if tokens[i + 1] == Tokens::OpenParen {
                            let function_params_tokens = tokens[i + 1..].to_vec();
                            let function_params_value =
                                ExpressionValue::get_expression_value(function_params_tokens)?;
                            output = Ok(Expression::FunctionCall(
                                word.clone(),
                                Box::new(function_params_value),
                            ));
                        }
                    }
                }
                _ => {}
            };
        }
        return output;
    }
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Debug)]
pub enum ExpressionValue {
    Value(EntryValue),
    TableReference(String),
    SubExpression(Expression),
}

impl ExpressionValue {
    fn get_expression_value(tokens: Vec<Tokens>) -> Result<ExpressionValue, String> {
        let mut striped_tokens = tokens;
        while striped_tokens[0] == Tokens::OpenParen
            && striped_tokens[striped_tokens.len() - 1] == Tokens::CloseParen
        {
            striped_tokens = striped_tokens[1..striped_tokens.len() - 1].to_vec();
        }
        if striped_tokens.len() == 1 {
            return match striped_tokens[0].clone() {
                Tokens::Str(string_value) => {
                    Ok(ExpressionValue::Value(EntryValue::Str(string_value)))
                }
                Tokens::Number(string_version) => {
                    let conversion_result = match string_version.parse::<isize>() {
                        Ok(number) => number,
                        _ => Err(format!("issue converting integer {:?}", string_version))?,
                    };
                    Ok(ExpressionValue::Value(EntryValue::Integer(
                        conversion_result,
                    )))
                }
                Tokens::Word(word) => {
                    if word == "true" {
                        return Ok(ExpressionValue::Value(EntryValue::Bool(true)));
                    }
                    if word == "false" {
                        return Ok(ExpressionValue::Value(EntryValue::Bool(false)));
                    }

                    return Ok(ExpressionValue::TableReference(word));
                }
                _ => Err(format!(
                    "Unable to convert expression value from {:?}",
                    striped_tokens
                )),
            };
        } else {
            return Ok(ExpressionValue::SubExpression(Expression::from_tokens(
                striped_tokens,
            )?));
        }
    }
}

fn lex_expression(expression: &String) -> Vec<Tokens> {
    let mut expression = expression.to_owned();
    expression.push(' ');
    let mut tokens = vec![];
    let mut mode = LexingMode::Nothing;
    let mut token_buffer = String::new();
    for (i, c) in expression.chars().enumerate() {
        if mode == LexingMode::Number {
            if i == expression.len() - 1 && c.is_numeric() {
                token_buffer.push(c);
                tokens.push(Tokens::Number(token_buffer));
                token_buffer = String::new();
            } else if c.is_numeric() || c == '.' {
                token_buffer.push(c);
            } else {
                tokens.push(Tokens::Number(token_buffer));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }
        if mode == LexingMode::Word {
            if i == expression.len() - 1 && c.is_alphanumeric() {
                token_buffer.push(c);
                tokens.push(Tokens::Word(token_buffer));
                token_buffer = String::new();
            } else if c.is_alphanumeric() || c == '.' {
                token_buffer.push(c);
            } else {
                tokens.push(Tokens::Word(token_buffer));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }
        if mode == LexingMode::Comparison {
            if i == expression.len() - 1 {
                token_buffer.push(c);
                tokens.push(Tokens::Comparison(convert_string_to_compare(token_buffer)));
                token_buffer = String::new();
            } else if c == '=' || c == '<' || c == '>' || c == '|' || c == '&' {
                token_buffer.push(c);
            } else {
                tokens.push(Tokens::Comparison(convert_string_to_compare(token_buffer)));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }
        if mode == LexingMode::Str {
            if i == expression.len() - 1 {
                tokens.push(Tokens::Str(token_buffer));
                token_buffer = String::new();
            } else if c != '"' {
                token_buffer.push(c);
            } else {
                tokens.push(Tokens::Str(token_buffer));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }

        if mode == LexingMode::Nothing {
            if c.is_alphabetic() {
                token_buffer.push(c);
                mode = LexingMode::Word;
            } else if c == '"' {
                mode = LexingMode::Str;
            } else if c == '=' || c == '<' || c == '>' || c == '|' || c == '&' || c == '!' {
                token_buffer.push(c);
                mode = LexingMode::Comparison;
            } else if c.is_numeric() || c == '.' {
                token_buffer.push(c);
                mode = LexingMode::Number;
            } else if c == '*' || c == '/' || c == '+' || c == '-' || c == '^' {
                tokens.push(Tokens::Operator(convert_char_to_operation(c)));
            } else if c == '~' {
                tokens.push(Tokens::Assign);
            } else if c == '(' {
                tokens.push(Tokens::OpenParen);
            } else if c == ')' {
                tokens.push(Tokens::CloseParen);
            }
        }
    }
    return tokens;
}

fn convert_string_to_compare(expr: String) -> Comparison {
    match expr.as_str() {
        "==" => Comparison::Eq,
        "!=" => Comparison::Neq,
        "<=" => Comparison::Lte,
        ">=" => Comparison::Gte,
        "<" => Comparison::Lt,
        ">" => Comparison::Gt,
        "&&" => Comparison::And,
        "||" => Comparison::Or,
        _ => panic!("Unknown comparison {}", expr),
    }
}

fn convert_char_to_operation(op: char) -> Operation {
    match op {
        '+' => Operation::Add,
        '*' => Operation::Mult,
        '/' => Operation::Div,
        '-' => Operation::Sub,
        '^' => Operation::Exp,
        _ => panic!("Unknown operation {}", op),
    }
}

#[derive(Eq, PartialEq)]
enum LexingMode {
    Word,
    Nothing,
    Number,
    Str,
    Comparison,
}
