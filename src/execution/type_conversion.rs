/// Centralized type conversion utilities for dtype-aware architecture
/// This ensures all operators respect schema types when creating arrays
use crate::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

/// Convert a vector of Values to an Arrow array, using the schema field type
/// This ensures arrays always match their declared schema types
pub fn values_to_array(
    values: Vec<Value>,
    data_type: &DataType,
    field_name: &str,
) -> Result<Arc<dyn Array>, anyhow::Error> {
    match data_type {
        DataType::Int64 => {
            let array_values: Vec<Option<i64>> = values.iter().map(|v| {
                match v {
                    Value::Int64(x) => Some(*x),
                    Value::Int32(x) => Some(*x as i64),
                    Value::Float64(f) => Some(*f as i64),  // Cast if needed
                    Value::Float32(f) => Some(*f as i64),
                    Value::Null => None,
                    _ => {
                        // Try to parse as integer if it's a string representation
                        if let Value::String(s) = v {
                            s.parse::<i64>().ok()
                        } else {
                            None
                        }
                    }
                }
            }).collect();
            Ok(Arc::new(Int64Array::from(array_values)) as Arc<dyn Array>)
        }
        DataType::Float64 => {
            let array_values: Vec<Option<f64>> = values.iter().map(|v| {
                match v {
                    Value::Float64(f) => Some(*f),
                    Value::Float32(f) => Some(*f as f64),
                    Value::Int64(i) => Some(*i as f64),  // Cast if needed
                    Value::Int32(i) => Some(*i as f64),
                    Value::Null => None,
                    _ => {
                        // Try to parse as float if it's a string representation
                        if let Value::String(s) = v {
                            s.parse::<f64>().ok()
                        } else {
                            None
                        }
                    }
                }
            }).collect();
            Ok(Arc::new(Float64Array::from(array_values)) as Arc<dyn Array>)
        }
        DataType::Utf8 => {
            let array_values: Vec<String> = values.iter().map(|v| {
                match v {
                    Value::String(s) => s.clone(),
                    Value::Int64(i) => i.to_string(),
                    Value::Int32(i) => i.to_string(),
                    Value::Float64(f) => format!("{}", f),
                    Value::Float32(f) => format!("{}", f),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                    Value::Vector(_) => format!("[vector]"),
                    _ => format!("{:?}", v),
                }
            }).collect();
            Ok(Arc::new(StringArray::from(array_values)) as Arc<dyn Array>)
        }
        DataType::Boolean => {
            let array_values: Vec<Option<bool>> = values.iter().map(|v| {
                match v {
                    Value::Bool(b) => Some(*b),
                    Value::Null => None,
                    _ => None,  // Can't convert other types to bool
                }
            }).collect();
            Ok(Arc::new(BooleanArray::from(array_values)) as Arc<dyn Array>)
        }
        _ => {
            anyhow::bail!("Unsupported data type {:?} for field '{}'", data_type, field_name)
        }
    }
}

/// Validate that an array matches the expected schema type
/// Returns error if types don't match (useful for debugging)
pub fn validate_array_type(
    array: &Arc<dyn Array>,
    expected_type: &DataType,
    field_name: &str,
) -> Result<(), anyhow::Error> {
    let actual_type = array.data_type();
    if actual_type != expected_type {
        anyhow::bail!(
            "Type mismatch for field '{}': expected {:?}, got {:?}",
            field_name,
            expected_type,
            actual_type
        );
    }
    Ok(())
}

/// Get the Arrow DataType that an aggregate function should return
/// This centralizes type rules for aggregates
pub fn aggregate_return_type(function: &crate::query::plan::AggregateFunction) -> DataType {
    use crate::query::plan::AggregateFunction;
    match function {
        AggregateFunction::Count | AggregateFunction::CountDistinct => DataType::Int64,
        AggregateFunction::Sum | AggregateFunction::Avg => DataType::Float64,
        AggregateFunction::Min | AggregateFunction::Max => {
            // MIN/MAX return the same type as input, but we default to Utf8 for now
            // TODO: Track input type in planner metadata
            DataType::Utf8
        }
    }
}

