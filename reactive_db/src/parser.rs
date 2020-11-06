#[derive(Eq, PartialEq)]
pub enum LexingMode {
    Word,
    Nothing,
    Number,
    Operator,
    Comparison
}

#[derive(Eq, PartialEq, Debug)]
pub enum Tokens {
    Word(String),
    OpenParen,
    CloseParen,
    Number(String),
    Assign,
    Operator(Operation),
    Comparison(Comparison)
}
#[derive(Eq, PartialEq, Debug)]
pub enum Comparison {
    Lt,
    Gt,
    Gte,
    Lte,
    Eq,
    Neq,
    Or,
    And
}
#[derive(Eq, PartialEq, Debug)]
pub enum Operation {
    Mult,
    Div,
    Add,
    Sub,
    Exp
}

pub fn lex_expression(expression: String) -> Vec<Tokens> {
    let mut tokens = vec![];
    let mut mode = LexingMode::Nothing;
    let mut token_buffer = String::new();
    for (i,c) in expression.chars().enumerate() {
        if mode == LexingMode::Number {
            if i == expression.len()-1 {
                token_buffer.push(c);
                tokens.push(Tokens::Number(token_buffer));
                token_buffer = String::new();
            }
            else if c.is_numeric() || c == '.' {
                token_buffer.push(c);
            }
            else {
                tokens.push(Tokens::Number(token_buffer));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }
        if mode == LexingMode::Word {
            if i == expression.len()-1 {
                token_buffer.push(c);
                tokens.push(Tokens::Word(token_buffer));
                token_buffer = String::new();
            }
            else if c.is_alphanumeric() {
                token_buffer.push(c);
            }
            else {
                tokens.push(Tokens::Word(token_buffer));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }
        if mode == LexingMode::Comparison {
            if i == expression.len()-1 {
                token_buffer.push(c);
                tokens.push(Tokens::Comparison(convert_string_to_compare(token_buffer)));
                token_buffer = String::new();
            }
            else if c == '=' || c == '<' || c == '>' || c == '|' || c == '&' {
                token_buffer.push(c);
            }
            else {
                tokens.push(Tokens::Comparison(convert_string_to_compare(token_buffer)));
                token_buffer = String::new();
                mode = LexingMode::Nothing;
            }
        }
        if mode == LexingMode::Nothing {
            if c.is_alphabetic() {
                token_buffer.push(c);
                mode = LexingMode::Word
            }
            else if c == '=' || c == '<' || c == '>' || c == '|' || c == '&' || c == '!' {
                token_buffer.push(c);
                mode = LexingMode::Comparison
            }
            else if c.is_numeric() || c == '.' {
                token_buffer.push(c);
                mode = LexingMode::Number
            }
            else if c == '*' || c == '/' || c == '+' || c == '-' || c == '^' {
                tokens.push(Tokens::Operator(convert_char_to_operation(c)));
            }
            else if  c == ':' {
                tokens.push(Tokens::Assign);
            }
            else if c == '(' {
                tokens.push(Tokens::OpenParen);
            }
            else if c == ')' {
                tokens.push(Tokens::CloseParen);
            }
        }
    }
    return tokens;
}

fn convert_string_to_compare(expr: String) -> Comparison{
    match expr.as_str() {
        "==" => Comparison::Eq,
        "!=" => Comparison::Neq,
        "<=" => Comparison::Neq,
        ">=" => Comparison::Neq,
        "<" => Comparison::Neq,
        ">" => Comparison::Neq,
        "&&" => Comparison::Neq,
        "||" => Comparison::Neq,
        _ => panic!("Unknown comparison {}", expr)
    }
}

fn convert_char_to_operation(op: char) -> Operation{
    match op {
        '+' => Operation::Add,
        '*' => Operation::Add,
        '/' => Operation::Add,
        '-' => Operation::Add,
        '^' => Operation::Add,
        _ => panic!("Unknown operation {}", op)
    }
}
