use query::*;
use random::Source;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;

mod query;
#[cfg(test)]
mod tests;

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

fn execute_program(program: &mut Program) -> Result<Records, String> {
    let mut stack = Vec::new();
    let mut records: Records = HashMap::new();
    let mut result: Records = HashMap::new();
    let mut rng = random::default(42);
    let mut i = 0;

    let mut it = 0;

    while i < program.len() {
        let op = &program[i];

        match op {
            Operation::Start => {
                i = i + 1;
            }
            Operation::End => {
                i = i + 1;
            }
            Operation::Push(value) => {
                stack.push(value.clone());
                i = i + 1;
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
                    val => {
                        return Err(format!("Record Id must be an id found {:#?}", val).to_owned())
                    }
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
                i = i + 1;
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
                i = i + 1;
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
                i = i + 1;
            }
            Operation::SelectAll => {
                for (record_id, record) in &records {
                    result.insert(*record_id, record.clone());
                }
                i = i + 1;
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
                        if field.key == key && predicate(&field.value, &value) {
                            include = true;
                            break;
                        }
                    }

                    if include {
                        result.insert(*record_id, record.clone());
                    }
                }
                i = i + 1;
            }
            Operation::Drop => {
                // stack must contain values
                // Any
                assert_stack_len(&stack, 1)?;

                stack.pop();
                i = i + 1;
            }
            Operation::It => {
                stack.push(Value::Int(it));
                i = i + 1;
            }
            Operation::Range { value, end } => {
                if *value > 0 {
                    it = *value;
                    program[i] = Operation::Range {
                        value: value - 1,
                        end: *end,
                    };
                    i = i + 1;
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

fn print_records(records: &Records) {
    for (record_id, record) in records {
        println!("________________________");
        println!("{0: <10} | {1: <10}", "Id", record_id);
        for field in &record.fields {
            match field.value.clone() {
                Value::Id(val) => {
                    println!("{0: <10} | {1: <10?}", field.key, val);
                }
                Value::Int(val) => {
                    println!("{0: <10} | {1: <10?}", field.key, val);
                }
                Value::Float(val) => {
                    println!("{0: <10} | {1: <10?}", field.key, val);
                }
                Value::String(val) => {
                    println!("{0: <10} | {1: <10?}", field.key, val);
                }
            }
        }
        println!("________________________");
    }
}

pub fn run() -> Result<(), String> {
    let contents = fs::read_to_string("hello.real").unwrap();
    let mut program = query::parse_program(contents)?;
    let records = execute_program(&mut program)?;

    print_records(&records);

    Ok(())
}
