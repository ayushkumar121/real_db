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

fn assert_stack_len(stack: &Vec<Value>, n: usize) -> Result<(), String> {
    if stack.len() < n {
        return Err(format!(
            "Stack must have atleast {} value(s), current stack is {:#?}",
            n, stack
        )
        .to_owned());
    }
    Ok(())
}

fn match_filter_predicate(predicate: Value) -> Option<fn(&Value, &Value) -> bool> {
    match predicate {
        Value::String(predicate) => match predicate.as_str() {
            "==" => Some(|a, b| a == b),
            "<" => Some(|a, b| a < b),
            "<=" => Some(|a, b| a <= b),
            ">" => Some(|a, b| a > b),
            ">=" => Some(|a, b| a >= b),
            _ => None,
        },
        _ => None,
    }
}
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
                // stack must contain values
                //  Key, Value
                assert_stack_len(&stack, 2)?;

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

                // Pushing inserted Id on the stack
                stack.push(Value::Id(record_id));
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
                // stack must contain values
                // Key, Value, Predicate
                assert_stack_len(&stack, 3)?;

                let predicate = match match_filter_predicate(stack.pop().unwrap()) {
                    Some(predicate) => predicate,
                    _ => return Err("Predicate unknown".to_owned()),
                };
                let value = stack.pop().unwrap();
                let key = match stack.pop().unwrap() {
                    Value::String(str) => str,
                    _ => return Err("Key must be a string".to_owned()),
                };

                // TODO: optimize this with indexes
                for (record_id, record) in &records {
                    let mut include = false;
                    for field in &record.fields {
                        if field.key == key && predicate(&value, &field.value) {
                            include = true;
                            break;
                        }
                    }

                    if include {
                        result.insert(*record_id, record.clone());
                    }
                }
            }
            Operation::Drop => {
                // stack must contain values
                // Any
                assert_stack_len(&stack, 1)?;

                stack.pop();
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
