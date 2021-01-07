use std::cmp::Ordering;

use rust_decimal::{Decimal, prelude::FromPrimitive};

use crate::types::EntryValue;

#[derive(Eq, PartialEq, Debug, Clone, Ord, PartialOrd)]
pub enum Comparison {
    Lt,
    Gt,
    Gte,
    Lte,
    Eq,
    Neq,
    Or,
    And,
}

impl Comparison {
    fn evaluate(&self, left: EntryValue, right: EntryValue) -> Result<EntryValue, String> {
        match (left, right) {
            (EntryValue::Integer(x), EntryValue::Integer(y)) => {
                Ok(self.does_ordering_pass(x.cmp(&y)))
            }
            (EntryValue::Str(x), EntryValue::Str(y)) => Ok(self.does_ordering_pass(x.cmp(&y))),
            (EntryValue::Array(x), EntryValue::Array(y)) => Ok(self.does_ordering_pass(x.cmp(&y))),
            (EntryValue::Bool(x), EntryValue::Bool(y)) => match self {
                Comparison::And => Ok(EntryValue::Bool(x && y)),
                Comparison::Or => Ok(EntryValue::Bool(x || y)),
                Comparison::Eq => Ok(EntryValue::Bool(x == y)),
                Comparison::Neq => Ok(EntryValue::Bool(x != y)),
                _ => Err(format!(
                    "Unable to do operation {:?} on {:?} and {:?}",
                    self, x, y
                )),
            },
            (left, right) => Err(format!(
                "Unable to do operation {:?} on {:?} and {:?}",
                self, left, right
            )),
        }
    }
    fn does_ordering_pass(&self, ord: Ordering) -> EntryValue {
        let t = EntryValue::Bool(true);
        let f = EntryValue::Bool(false);
        match (self, ord) {
            (Comparison::Lt, Ordering::Less) => t,
            (Comparison::Gt, Ordering::Greater) => t,
            (Comparison::Gte, Ordering::Greater) => t,
            (Comparison::Gte, Ordering::Equal) => t,
            (Comparison::Lte, Ordering::Less) => t,
            (Comparison::Lte, Ordering::Equal) => t,
            (Comparison::Eq, Ordering::Equal) => t,
            (Comparison::Neq, Ordering::Less) => t,
            (Comparison::Neq, Ordering::Greater) => t,
            _ => f,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Ord, PartialOrd)]
pub enum Operation {
    Mult,
    Div,
    Add,
    Sub,
    Exp,
}

impl Operation {
    fn evaluate(&self, left: EntryValue, right: EntryValue) -> Result<EntryValue, String> {
        match (left, right) {
            (EntryValue::Integer(x), EntryValue::Integer(y)) => match self {
                Operation::Mult => Ok(EntryValue::Integer(x * y)),
                Operation::Div => Ok(EntryValue::Integer(x / y)),
                Operation::Add => Ok(EntryValue::Integer(x + y)),
                Operation::Sub => Ok(EntryValue::Integer(x - y)),
                Operation::Exp => Ok(EntryValue::Integer(x.pow(y.abs() as u32))),
            },
            (EntryValue::Decimal(x), EntryValue::Decimal(y)) => match self {
                Operation::Mult => Ok(EntryValue::Decimal(x * y)),
                Operation::Div => Ok(EntryValue::Decimal(x / y)),
                Operation::Add => Ok(EntryValue::Decimal(x + y)),
                Operation::Sub => Ok(EntryValue::Decimal(x - y)),
                Operation::Exp => Err("Exponent not supported for decimals".to_string())
            },
            (EntryValue::Decimal(x), EntryValue::Integer(y)) => self.evaluate(EntryValue::Decimal(x), EntryValue::Decimal(Decimal::from_isize(y).unwrap())),
            (EntryValue::Integer(x), EntryValue::Decimal(y)) => self.evaluate(EntryValue::Decimal(Decimal::from_isize(x).unwrap()), EntryValue::Decimal(y)),
            (EntryValue::Str(x), EntryValue::Str(y)) => match self {
                Operation::Add => {
                    let mut out = x.to_owned();
                    out.push_str(y.as_str());
                    return Ok(EntryValue::Str(x));
                }
                _ => Err("Only add is a valid string operation".to_string()),
            },
            (left, right) => Err(format!(
                "Unable to do operation {:?} on {:?} and {:?}",
                self, left, right
            )),
        }
    }
}

#[derive(Debug, Ord, Eq, PartialOrd, PartialEq, Clone)]
pub enum OperationOrComparison {
    Operation(Operation),
    Comparison(Comparison),
}

impl OperationOrComparison {
    pub fn evaluate(&self, left: EntryValue, right: EntryValue) -> Result<EntryValue, String> {
        match self {
            OperationOrComparison::Operation(op) => op.evaluate(left, right),
            OperationOrComparison::Comparison(cmp) => cmp.evaluate(left, right),
        }
    }
}