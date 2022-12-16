use crate::{Id, QueryResult, Record, Records, Value};
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

pub fn set(records: &mut Records, id: Id, key: String, value: Value) {
    let key = key.to_lowercase();
    if let Some(record) = records.get_mut(&id) {
        record.fields.insert(key, value);
    } else {
        records.insert(
            id,
            Record {
                fields: HashMap::from([(String::from("id"), Value::Id(id)), (key, value)]),
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
    for (record_id, record) in records.iter_mut() {
        let mut include = false;
        for (field_key, field_value) in &record.fields {
            if field_key.eq(&key) && predicate(field_value, &value) {
                include = true;
                break;
            }
        }

        if include {
            result.push(*record_id);
        }
    }
}
