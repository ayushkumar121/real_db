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

pub type Id = u64;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value {
    Id(Id),
    Int(i64),
    Float(f64),
    String(String),
}

type Records = HashMap<Id, Record>;
type QueryResult = Vec<Id>;

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
fn execute_program(
    records: Arc<Mutex<Records>>,
    mut program: Program,
) -> Result<QueryResult, String> {
    let records = &mut records.lock().unwrap();

    let mut stack = Vec::new();
    let mut result = Vec::new();
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
                    Value::Id(id) => id,
                    Value::Int(num) => num as Id,
                    val => return Err(format!("Record Id must be an id found {:#?}", val)),
                };

                intrinsics::set(records, record_id, key, value);
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
                let record_id: Id = rng.read_u64();

                intrinsics::set(records, record_id, key, value);

                // Pushing inserted Id on the stack
                stack.push(Value::Id(record_id));
                i += 1;
            }
            Operation::Select => {
                // stack must contain values
                // Id
                assert_stack_len(&stack, 1)?;

                let record_id = match stack.pop().unwrap() {
                    Value::Id(id) => id,
                    Value::Int(num) => num as Id,
                    _ => return Err("Record Id must be an id".to_owned()),
                };

                if records.get(&record_id).is_some() {
                    result.push(record_id);
                } else {
                    return Err("Record not found".to_owned());
                }
                i += 1;
            }
            Operation::SelectAll => {
                for (record_id, _) in records.iter_mut() {
                    result.push(*record_id);
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
                    Value::Id(val) => val as i64,
                    Value::Int(val) => val,
                    _ => return Err("Add requires two int on stack".to_owned()),
                };

                let a = match stack.pop().unwrap() {
                    Value::Id(val) => val as i64,
                    Value::Int(val) => val,
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
                    Value::Id(val) => val as i64,
                    Value::Int(val) => val,
                    _ => return Err("Add requires two int on stack".to_owned()),
                };

                let a = match stack.pop().unwrap() {
                    Value::Id(val) => val as i64,
                    Value::Int(val) => val,
                    _ => return Err("Add requires two int on stack".to_owned()),
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

fn results_to_json(records: Arc<Mutex<Records>>, result: QueryResult) -> String {
    let records = records.lock().unwrap();
    let mut output = String::new();
    output.push_str("{\n\"message\":\"OK\",\n\"data\": [\n");

    for (row, id) in result.iter().enumerate() {
        let record = records.get(id).unwrap();
        output.push_str("{\n");

        for (i, (key, value)) in record.fields.iter().enumerate() {
            match value {
                Value::Id(val) => {
                    output.push_str(&format!("\"{}\":{}", key, val));
                }
                Value::Int(val) => {
                    output.push_str(&format!("\"{}\":{}", key, val));
                }
                Value::Float(val) => {
                    output.push_str(&format!("\"{}\":{}", key, val));
                }
                Value::String(val) => {
                    output.push_str(&format!("\"{}\":{}", key, val));
                }
            }
            if i != record.fields.len() - 1 {
                output.push_str(",");
            }
            output.push_str("\n");
        }

        if row == result.len() - 1 {
            output.push_str("}]\n");
        } else {
            output.push_str("},\n");
        }
    }
    output.push_str("}\n");
    output
}

fn report_err(err: String, mut stream: TcpStream) {
    let json = format!("{{\"message\":\"{}\"}}", err);

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
        json.len(),
        json
    );

    stream.write_all(response.as_bytes()).unwrap();
    stream.flush().unwrap();

    println!("\x1b[0;31mError : {}\x1b[0m", err);
}

fn handle_query(
    records: Arc<Mutex<Records>>,
    mut stream: TcpStream,
) -> impl FnOnce() + Send + 'static {
    move || {
        let buf_reader = BufReader::new(&mut stream);
        let program = match query::parse_tcp(buf_reader) {
            Ok(val) => val,
            Err(err) => {
                report_err(err, stream);
                return;
            }
        };

        let result = match execute_program(Arc::clone(&records), program) {
            Ok(val) => val,
            Err(err) => {
                report_err(err, stream);
                return;
            }
        };

        let json = results_to_json(Arc::clone(&records), result);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            json.len(),
            json
        );

        stream.write_all(response.as_bytes()).unwrap();
        stream.flush().unwrap();
    }
}

pub fn run() -> Result<(), String> {
    // Database
    let records: Records = HashMap::new();
    let records = Arc::new(Mutex::new(records));

    let listener = TcpListener::bind("127.0.0.1:1234").unwrap();
    let pool = ThreadPool::new(4);

    println!("Database is listening to http://localhost:1234");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        pool.execute(handle_query(Arc::clone(&records), stream));
    }

    Ok(())
}
