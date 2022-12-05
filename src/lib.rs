use random::Source;
use std::{collections::HashMap, fs};

#[derive(Debug)]
struct Token {
    word: String,
    kind: TokenKind,
}

#[derive(Debug)]
enum TokenKind {
    Set,
    Select,
    Auto,
    Int,
    Float,
    String,
    Word,
}

fn match_token_kind(word: &str) -> TokenKind {
    let word = word.to_lowercase();
    match word.as_str() {
        "_" => TokenKind::Auto,
        "set" => TokenKind::Set,
        "select" => TokenKind::Select,
        _ => {
            if word.starts_with('\"') && word.ends_with('\"') {
                return TokenKind::String;
            }

            if word.parse::<i64>().is_ok() {
                return TokenKind::Int;
            }

            if word.parse::<f64>().is_ok() {
                return TokenKind::Float;
            }

            TokenKind::Word
        }
    }
}

fn parse_file(file_name: &str) -> Result<Vec<Token>, String> {
    let contents = match fs::read_to_string(file_name) {
        Ok(val) => val,
        Err(e) => return Err(e.kind().to_string()),
    };

    fn flush_token(tokens: &mut Vec<Token>, word: &mut String) {
        let w = word.trim();

        if w.is_empty() {
            return;
        }

        tokens.push(Token {
            word: w.to_owned(),
            kind: match_token_kind(w),
        });

        word.clear();
    }

    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut is_str = false;

    for ch in contents.chars() {
        match ch {
            ' ' | '\n' | '\r' => {
                if !is_str {
                    flush_token(&mut tokens, &mut word);
                }
            }
            '\"' => {
                is_str = !is_str;
            }
            _ => {}
        }

        word.push(ch);
    }

    flush_token(&mut tokens, &mut word);
    Ok(tokens)
}

type Id = u64;

#[derive(Debug)]
enum Value {
    Id(Id),
    Int(i64),
    Float(f64),
    String(String),
}

enum Operation {
    Push(Value),
    Set,
    Select,
}

type Program = Vec<Operation>;

fn parse_program(tokens: Vec<Token>) -> Result<Program, String> {
    let mut program = Vec::new();
    let mut source = random::default(42);

    for token in tokens {
        match token.kind {
            TokenKind::String => {
                let mut w = token.word.strip_prefix('\"').unwrap();
                w = w.strip_suffix('\"').unwrap();

                program.push(Operation::Push(Value::String(w.to_owned())));
            }
            TokenKind::Int => {
                program.push(Operation::Push(Value::Int(token.word.parse().unwrap())))
            }
            TokenKind::Float => {
                program.push(Operation::Push(Value::Float(token.word.parse().unwrap())))
            }
            TokenKind::Auto => {
                let n: Id = source.read_u64();
                program.push(Operation::Push(Value::Id(n)));
            }
            TokenKind::Set => program.push(Operation::Set),
            TokenKind::Select => program.push(Operation::Select),
            _ => return Err(format!("unknown word `{}`", token.word).to_owned()),
        }
    }

    Ok(program)
}

#[derive(Debug)]
struct Field {
    key: String,
    value: Value,
}

#[derive(Debug)]
struct Record {
    fields: Vec<Field>,
}

fn execute_program(program: Program) -> Result<(), String> {
    let mut stack = Vec::new();
    let mut records: HashMap<Id, Record> = HashMap::new();

    for op in program {
        match op {
            Operation::Push(value) => {
                stack.push(value);
            }
            Operation::Set => {
                // stack must contain 3 values
                // Id, Key, Value
                if stack.len() < 3 {
                    return Err(
                        "Stack must have atleast 3 values, current stack is {:#?}".to_owned()
                    );
                }
                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };
                let record_id = match stack.pop().unwrap() {
                    Value::Id(id) => id,
                    Value::Int(num) => num as Id,
                    _ => return Err("Record Id must be an id".to_owned()),
                };
                let new_field = Field { key, value };

                if let Some(record) = records.get_mut(&record_id) {
                    record.fields.push(new_field);
                } else {
                    records.insert(
                        record_id,
                        Record {
                            fields: vec![new_field],
                        },
                    );
                }
            }
            Operation::Select => {
                // stack must contain 1 values
                // Id
                if stack.len() < 1 {
                    return Err(
                        "Stack must have atleast 1 values, current stack is {:#?}".to_owned()
                    );
                }
                let record_id = match stack.pop().unwrap() {
                    Value::Id(id) => id,
                    Value::Int(num) => num as Id,
                    _ => return Err("Record Id must be an id".to_owned()),
                };

                if let Some(record) = records.get(&record_id) {
                    println!("**********************");
                    println!("Id={:#?}", record_id);
                    for field in &record.fields {
                        println!("{}={:#?}", field.key, field.value);
                    }
                    println!("**********************");
                } else {
                    return Err("Record not found".to_owned());
                }
            }
        }
    }
    Ok(())
}

pub fn run() -> Result<(), String> {
    let tokens = parse_file("hello.real")?;
    let program = parse_program(tokens)?;
    execute_program(program)?;

    Ok(())
}
