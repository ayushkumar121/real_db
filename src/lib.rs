use query::*;
use random::Source;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;

mod query;

#[derive(Debug)]
struct Field {
    key: String,
    value: Value,
}

#[derive(Debug)]
struct Record {
    fields: Vec<Field>,
}

type Records = HashMap<Id, Rc<Record>>;

fn execute_program(program: Program) -> Result<Records, String> {
    let mut stack = Vec::new();
    let mut records: Records = HashMap::new();
    let mut result: Records = HashMap::new();
    let mut rng = random::default(42);

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
                    let record = Rc::get_mut(record).unwrap();
                    record.fields.push(new_field);
                } else {
                    records.insert(
                        record_id,
                        Rc::new(Record {
                            fields: vec![new_field],
                        }),
                    );
                }
            }
            Operation::Insert => {
                // stack must contain 3 values
                //  Key, Value
                if stack.len() < 2 {
                    return Err(
                        "Stack must have atleast 2 values, current stack is {:#?}".to_owned()
                    );
                }
                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };
                let new_field = Field { key, value };
                let record_id: Id = rng.read_u64();

                if let Some(record) = records.get_mut(&record_id) {
                    let record = Rc::get_mut(record).unwrap();
                    record.fields.push(new_field);
                } else {
                    records.insert(
                        record_id,
                        Rc::new(Record {
                            fields: vec![new_field],
                        }),
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
                    result.insert(record_id, record.clone());
                } else {
                    return Err("Record not found".to_owned());
                }
            }
            Operation::SelectAll => {
                for (record_id, record) in &records {
                    result.insert(*record_id, record.clone());
                }
            }
            Operation::Filter => {
                // stack must contain 3 values
                // Key, Value, Predicate
                if stack.len() < 3 {
                    return Err(
                        "Stack must have atleast 3 values, current stack is {:#?}".to_owned()
                    );
                }

                let _predicate = match stack.pop().unwrap() {
                    Value::String(predicate) => match predicate.as_str() {
                        "==" => predicate,
                        _ => return Err(format!("Unknown predicated `{}`", predicate).to_owned()),
                    },
                    _ => return Err("Record Id must be an id".to_owned()),
                };
                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };

                for (record_id, record) in &records {
                    let mut include = false;
                    for field in &record.fields {
                        if field.key == key && field.value == value {
                            include = true;
                            break;
                        }
                    }

                    if include {
                        result.insert(*record_id, record.clone());
                    }
                }
            }
        }
    }

    Ok(result)
}

pub fn run() -> Result<(), String> {
    let contents = fs::read_to_string("hello.real").unwrap();
    let program = query::parse_program(contents)?;
    let result = execute_program(program)?;

    for (record_id, record) in &result {
        println!("**********************");
        println!("Id={:#?}", record_id);
        for field in &record.fields {
            println!("{}={:#?}", field.key, field.value);
        }
        println!("**********************");
    }

    Ok(())
}
