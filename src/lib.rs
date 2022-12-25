use query::*;
use random::Source;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, io::BufReader};

use crate::server::ThreadPool;

mod intrinsics;
mod query;
mod server;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Record {
    fields: HashMap<String, Value>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RecordId {
    table_name: String,
    row: u64,
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value {
    // Id is composed of a table name and row id
    Id(RecordId),
    Int(i64),
    Float(f64),
    String(String),
    // Array
    // RecordLink(Id),
}

type Records = HashMap<u64, Record>;
type QueryResult = Vec<u64>;

pub struct Database {
    // Hashmap from table name to records
    tables: HashMap<String, Records>,
}
pub type DatabaseRef = Arc<Mutex<Database>>;

const DEFAULT_TABLE: &str = "0";

fn assert_stack_len(stack: &Vec<Value>, n: usize) -> Result<(), String> {
    if stack.len() < n {
        return Err(format!(
            "Stack must have atleast {} value(s), current stack is {:#?}",
            n, stack
        ));
    }
    Ok(())
}

// Query Execution
fn execute_program(database: DatabaseRef, mut program: Program) -> Result<QueryResult, String> {
    let mut database = database.lock().unwrap();
    let records = database.tables.get_mut(DEFAULT_TABLE).unwrap();

    let mut stack = Vec::new();
    let mut result: QueryResult = Vec::new();
    let mut i = 0;

    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let mut rng = random::default(since_the_epoch.as_secs());

    let mut it = 0;

    while i < program.len() {
        let op = &program[i];

        match op {
            Operation::Start => {
                i += 1;
            }
            Operation::End => {
                i += 1;
            }
            Operation::Push(value) => {
                stack.push(value.clone());
                i += 1;
            }
            Operation::Set => {
                // stack must contain values
                // Id, Key, Value
                assert_stack_len(&stack, 3)?;

                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };
                let record_id = match stack.pop().unwrap() {
                    Value::Id(record_id) => record_id,
                    val => return Err(format!("Record Id must be an id found {:#?}", val)),
                };

                intrinsics::set(records, &record_id, key, value);
                i += 1;
            }
            Operation::Insert => {
                // stack must contain values
                //  Key, Value
                assert_stack_len(&stack, 2)?;

                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };
                let row: u64 = rng.read_u64();
                let record_id = RecordId {
                    table_name: DEFAULT_TABLE.to_lowercase(),
                    row,
                };

                intrinsics::set(records, &record_id, key, value);

                // Pushing inserted Id on the stack
                stack.push(Value::Id(record_id));
                i += 1;
            }
            Operation::Select => {
                // stack must contain values
                // Id
                assert_stack_len(&stack, 1)?;

                let record_id = match stack.pop().unwrap() {
                    Value::Id(record_id) => record_id,
                    _ => return Err("Record Id must be an id".to_owned()),
                };

                if records.get(&record_id.row).is_some() {
                    result.push(record_id.row);
                } else {
                    return Err("Record not found".to_owned());
                }
                i += 1;
            }
            Operation::SelectAll => {
                for (row, _) in records.iter_mut() {
                    result.push(*row);
                }
                i += 1;
            }
            Operation::Filter => {
                // stack must contain values
                // Key, Value, Predicate
                assert_stack_len(&stack, 3)?;

                let predicate = match intrinsics::match_predicate(stack.pop().unwrap()) {
                    Some(predicate) => predicate,
                    _ => return Err("Predicate unknown".to_owned()),
                };
                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };

                intrinsics::filter(records, &mut result, key, value, predicate);
                i += 1;
            }
            Operation::Drop => {
                // stack must contain values
                // Any
                assert_stack_len(&stack, 1)?;

                stack.pop();
                i += 1;
            }
            Operation::Add => {
                // stack must contain values
                // a:Int b:Int
                assert_stack_len(&stack, 2)?;

                let b = match stack.pop().unwrap() {
                    Value::Int(num) => num,
                    _ => return Err("Add requires two int on stack".to_owned()),
                };

                let a = match stack.pop().unwrap() {
                    Value::Int(num) => num,
                    _ => return Err("Add requires two int on stack".to_owned()),
                };

                stack.push(Value::Int(a + b));
                i += 1;
            }
            Operation::Subtract => {
                // stack must contain values
                // a:Int b:Int
                assert_stack_len(&stack, 2)?;
                let b = match stack.pop().unwrap() {
                    Value::Int(num) => num,
                    _ => return Err("Sub requires two int on stack".to_owned()),
                };

                let a = match stack.pop().unwrap() {
                    Value::Int(num) => num,
                    _ => return Err("Sub requires two int on stack".to_owned()),
                };

                stack.push(Value::Int(a - b));
                i += 1;
            }
            Operation::It => {
                stack.push(Value::Int(it));
                i += 1;
            }
            Operation::Range { value, end } => {
                if *value > 0 {
                    it = *value;
                    program[i] = Operation::Range {
                        value: value - 1,
                        end: *end,
                    };
                    i += 1;
                } else {
                    i = *end;
                }
            }
            Operation::Jump(pos) => {
                i = *pos;
            }
        }
    }

    Ok(result)
}

fn results_to_json(database: DatabaseRef, result: QueryResult) -> String {
    let database = database.lock().unwrap();
    let records = database.tables.get("0").unwrap();

    let mut output = String::new();
    output.push_str("{\n\"message\":\"OK\",\n\"data\": [\n");

    for (row, id) in result.iter().enumerate() {
        let record = records.get(id).unwrap();
        output.push_str("{\n");

        for (i, (key, value)) in record.fields.iter().enumerate() {
            match value {
                Value::Id(record_id) => {
                    output.push_str(&format!(
                        "\"{}\":\"{}:{}\"",
                        key, record_id.table_name, record_id.row
                    ));
                }
                Value::Int(val) => {
                    output.push_str(&format!("\"{}\":{}", key, val));
                }
                Value::Float(val) => {
                    output.push_str(&format!("\"{}\":{}", key, val));
                }
                Value::String(val) => {
                    output.push_str(&format!("\"{}\":\"{}\"", key, val));
                } // Recurvivly print the document
                  // Value::RecordLink(_) => {
                  // let record = records.get(id).unwrap();
                  // todo!()
                  // }
            }
            if i != record.fields.len() - 1 {
                output.push(',');
            }
            output.push('\n');
        }

        if row == result.len() - 1 {
            output.push('}');
        } else {
            output.push_str("},\n");
        }
    }
    output.push_str("]\n}\n");
    output
}

fn report_err(err: String, mut stream: TcpStream) {
    let json = format!("{{\"message\":\"{}\"}}", err);

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{}\r\n\r\n{}",
        json.len(),
        "Content-Type: application/json",
        json
    );

    stream.write_all(response.as_bytes()).unwrap();
    stream.flush().unwrap();

    println!("\x1b[0;31mError : {}\x1b[0m", err);
}

fn handle_query(database: DatabaseRef, mut stream: TcpStream) -> impl FnOnce() + Send + 'static {
    move || {
        let buf_reader = BufReader::new(&mut stream);
        let program = match query::parse_tcp(buf_reader) {
            Ok(val) => val,
            Err(err) => {
                report_err(err, stream);
                return;
            }
        };

        let result = match execute_program(Arc::clone(&database), program) {
            Ok(val) => val,
            Err(err) => {
                report_err(err, stream);
                return;
            }
        };

        let json = results_to_json(Arc::clone(&database), result);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{}\r\n\r\n{}",
            json.len(),
            "Content-Type: application/json",
            json
        );

        stream.write_all(response.as_bytes()).unwrap();
        stream.flush().unwrap();
    }
}

pub fn run() -> Result<(), String> {
    // Database
    let database = Database {
        tables: HashMap::from([(DEFAULT_TABLE.to_owned(), HashMap::new())]),
    };
    let database = Arc::new(Mutex::new(database));

    let listener = match TcpListener::bind("127.0.0.1:1234") {
        Ok(val) => val,
        Err(err) => return Err(format!("{}", err)),
    };

    let pool = ThreadPool::new(4);

    println!("Database is listening to http://localhost:1234");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        pool.execute(handle_query(Arc::clone(&database), stream));
    }

    Ok(())
}
