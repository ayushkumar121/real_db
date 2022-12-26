use crate::{QueryResult, Record, RecordId, Records, Value};
use std::collections::HashMap;

type FilterPredicate = fn(&Value, &Value) -> bool;

pub fn match_predicate(predicate: Value) -> Option<FilterPredicate> {
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

pub fn set(records: &mut Records, record_id: &RecordId, key: String, value: Value) {
    let key = key.to_lowercase();
    if let Some(record) = records.get_mut(&record_id.row) {
        record.fields.insert(key, value);
    } else {
        records.insert(
            record_id.row,
            Record {
                fields: HashMap::from([
                    (String::from("id"), Value::Id(record_id.clone())),
                    (key, value),
                ]),
            },
        );
    }
}

// TODO: optimize this with indexes
pub fn filter(
    records: &mut Records,
    result: &mut QueryResult,
    key: String,
    value: Value,
    predicate: FilterPredicate,
) {
    for (_, record) in records.iter() {
        let mut include = false;
        for (field_key, field_value) in &record.fields {
            if field_key.eq(&key) && predicate(field_value, &value) {
                include = true;
                break;
            }
        }

        if include {
            match record.fields.get("id").unwrap() {
                Value::Id(record_id) => {
                    result.push(record_id.clone());
                }
                _ => {}
            };
        }
    }
}
