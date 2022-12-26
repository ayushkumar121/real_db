use std::{
    io::{BufRead, BufReader, Read},
    net::TcpStream,
    time::{SystemTime, UNIX_EPOCH},
};

use random::Source;

use crate::{RecordId, Value};

#[derive(Debug)]
pub struct Token {
    word: String,
    kind: TokenKind,
    line: usize,
    col: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TokenKind {
    Plus,
    Minus,
    Set,
    Select,
    SelectAll,
    Filter,
    Drop,
    Id,
    Int,
    Float,
    String,
    Word,
    Range,
    It,
    Do,
    End,
}

fn match_token_kind(word: &str) -> TokenKind {
    let word = word.to_lowercase();
    match word.as_str() {
        "+" => TokenKind::Plus,
        "-" => TokenKind::Minus,
        "set" => TokenKind::Set,
        "select" => TokenKind::Select,
        "select_all" => TokenKind::SelectAll,
        "filter" => TokenKind::Filter,
        "drop" => TokenKind::Drop,
        "range" => TokenKind::Range,
        "it" => TokenKind::It,
        "do" => TokenKind::Do,
        "end" => TokenKind::End,
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

            // @table_name:1234
            if word.starts_with('@') && word.split(':').count() == 2 {
                return TokenKind::Id;
            }

            TokenKind::Word
        }
    }
}

fn tokenize(contents: String) -> Result<Vec<Token>, String> {
    fn flush_token(tokens: &mut Vec<Token>, word: &mut String, line: usize, col: usize) {
        let w = word.trim();

        if w.is_empty() {
            return;
        }

        tokens.push(Token {
            word: w.to_owned(),
            kind: match_token_kind(w),
            line,
            col,
        });

        word.clear();
    }

    let mut tokens = Vec::new();
    let mut word = String::new();

    let mut line = 1;
    let mut col = 1;

    let mut is_str = false;
    let mut is_comment = false;

    for ch in contents.chars() {
        match ch {
            ' ' | '\n' | '\r' => {
                if !is_str {
                    flush_token(&mut tokens, &mut word, line, col);
                }

                if ch == '\n' {
                    is_comment = false;
                    line += 1;
                    col = 0;
                }
            }
            '#' => is_comment = true,
            '\"' => {
                is_str = !is_str;
            }
            _ => {}
        }

        if !is_comment {
            word.push(ch);
            col += 1;
        }
    }

    flush_token(&mut tokens, &mut word, line, col);
    Ok(tokens)
}

pub enum Operation {
    // Start and end labels for query
    Start,
    End,
    Push(Value),
    // Instricts
    Set,
    Select,
    SelectAll,
    Filter,
    Drop,
    Add,
    Subtract,
    It,
    // Starts a range scope
    // Decide weather to jump to end or fallthrough
    Range { value: i64, end: usize },
    // Jumps back to start of the scope
    Jump(usize),
}

pub type Program = Vec<Operation>;

pub fn parse(contents: String) -> Result<Program, String> {
    let tokens = tokenize(contents)?;

    let mut program = vec![Operation::Start];
    let mut scopes = Vec::new();

    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let mut rng = random::default(since_the_epoch.as_secs());

    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];

        match token.kind {
            TokenKind::Id => {
                let parts: Vec<_> = token.word.split(':').collect();

                if parts.len() != 2 {
                    return Err(format!(
                        "Unexpected id format at line {}:{}. It should be like @table_name:1234",
                        token.line, token.col
                    ));
                }

                let table_name = parts[0];
                let r = parts[1];
                let row = match r {
                    "_" => rng.read_u64(),
                    _ => {
                        if let Ok(n) = r.parse::<u64>() {
                            n
                        } else {
                            return Err(format!(
                        "Unexpected id format at line {}:{}. It should be like :table_name:1234",
                        token.line, token.col));
                        }
                    }
                };

                program.push(Operation::Push(Value::Id(RecordId {
                    table_name: table_name.to_owned(),
                    row,
                })));
            }
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
            TokenKind::Set => program.push(Operation::Set),
            TokenKind::Select => program.push(Operation::Select),
            TokenKind::SelectAll => program.push(Operation::SelectAll),
            TokenKind::Filter => program.push(Operation::Filter),
            TokenKind::Drop => program.push(Operation::Drop),
            TokenKind::Plus => program.push(Operation::Add),
            TokenKind::Minus => program.push(Operation::Subtract),
            TokenKind::Range => {
                i += 1;
                let next_token = &tokens[i];
                if next_token.kind != TokenKind::Int {
                    return Err(format!(
                        "Expected int at not {} line {}:{}",
                        next_token.word, next_token.line, next_token.col
                    ));
                }
                let value: i64 = next_token.word.parse().unwrap();

                scopes.push(program.len());
                program.push(Operation::Range { value, end: 0 });
            }
            TokenKind::It => {
                if scopes.last().is_some() {
                    program.push(Operation::It);
                } else {
                    return Err(format!(
                        "Unexpected end without matching do line {}:{}",
                        token.line, token.col
                    ));
                }
            }
            TokenKind::Do => {}
            TokenKind::End => {
                if let Some(pos) = scopes.pop() {
                    let end = program.len() + 1;
                    program[pos] = match program[pos] {
                        Operation::Range { value, .. } => Operation::Range { value, end },
                        _ => todo!(),
                    };

                    program.push(Operation::Jump(pos));
                } else {
                    return Err(format!(
                        "Unexpected end without matching do line {}:{}",
                        token.line, token.col
                    ));
                }
            }
            TokenKind::Word => {
                return Err(format!(
                    "Unexpected word `{}` line {}:{}",
                    token.word.escape_default(),
                    token.line,
                    token.col
                ));
            }
        }

        i += 1;
    }
    program.push(Operation::End);

    Ok(program)
}

pub fn parse_tcp(mut buf_reader: BufReader<&mut TcpStream>) -> Result<Program, String> {
    // Reading Headers
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).unwrap();

    loop {
        let mut header_line = String::new();
        buf_reader.read_line(&mut header_line).unwrap();

        if header_line.trim() == "" {
            break;
        }
    }

    // Reading body
    // We expect a new line to be present at
    // end of the body

    // TODO: Parse multiline text
    let mut body = String::new();
    buf_reader.take(512).read_line(&mut body).unwrap();

    println!("Executing query: \x1b[1;95m{}\x1b[0m", body.trim());

    parse(body)
}
