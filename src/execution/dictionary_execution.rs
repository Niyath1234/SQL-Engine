/// Dictionary-encoded execution kernels (Phase 2)
/// Execute filters, joins, and groupby directly on dictionary codes
use crate::storage::fragment::{ColumnFragment, Value};
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;
use fxhash::FxHashMap;

/// Filter a dictionary-encoded fragment using integer codes
/// Returns a selection bitmap indicating which rows match
pub fn filter_dictionary_codes(
    codes: &Arc<dyn Array>,
    dictionary: &[Value],
    predicate_value: &Value,
    operator: &crate::query::plan::PredicateOperator,
) -> Result<BitVec> {
    // First, find the code(s) that match the predicate value
    let matching_codes = find_matching_codes(dictionary, predicate_value, operator)?;
    
    let row_count = codes.len();
    let mut selection = bitvec![0; row_count];
    
    // Check code type
    match codes.data_type() {
        DataType::UInt16 => {
            let arr = codes.as_any().downcast_ref::<UInt16Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to UInt16Array"))?;
            for i in 0..row_count {
                if !arr.is_null(i) {
                    let code = arr.value(i) as u32;
                    if matching_codes.contains(&code) {
                        selection.set(i, true);
                    }
                }
            }
        }
        DataType::UInt32 => {
            let arr = codes.as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to UInt32Array"))?;
            for i in 0..row_count {
                if !arr.is_null(i) {
                    let code = arr.value(i);
                    if matching_codes.contains(&code) {
                        selection.set(i, true);
                    }
                }
            }
        }
        _ => {
            anyhow::bail!("Dictionary codes must be UInt16 or UInt32");
        }
    }
    
    Ok(selection)
}

/// Find dictionary codes that match a predicate value
fn find_matching_codes(
    dictionary: &[Value],
    predicate_value: &Value,
    operator: &crate::query::plan::PredicateOperator,
) -> Result<std::collections::HashSet<u32>> {
    use crate::query::plan::PredicateOperator;
    let mut matching = std::collections::HashSet::new();
    
    match operator {
        PredicateOperator::Equals => {
            for (code, dict_val) in dictionary.iter().enumerate() {
                if dict_val == predicate_value {
                    matching.insert(code as u32);
                }
            }
        }
        PredicateOperator::NotEquals => {
            for (code, dict_val) in dictionary.iter().enumerate() {
                if dict_val != predicate_value {
                    matching.insert(code as u32);
                }
            }
        }
        PredicateOperator::LessThan | PredicateOperator::LessThanOrEqual |
        PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEqual => {
            // For comparison operators, we need to compare values
            for (code, dict_val) in dictionary.iter().enumerate() {
                if compare_values(dict_val, predicate_value, operator) {
                    matching.insert(code as u32);
                }
            }
        }
        _ => {
            // For other operators, fall back to checking all codes
            for code in 0..dictionary.len() {
                matching.insert(code as u32);
            }
        }
    }
    
    Ok(matching)
}

/// Compare two values based on operator
fn compare_values(
    left: &Value,
    right: &Value,
    operator: &crate::query::plan::PredicateOperator,
) -> bool {
    use crate::query::plan::PredicateOperator;
    
    match operator {
        PredicateOperator::LessThan => left < right,
        PredicateOperator::LessThanOrEqual => left <= right,
        PredicateOperator::GreaterThan => left > right,
        PredicateOperator::GreaterThanOrEqual => left >= right,
        _ => false,
    }
}

/// GroupBy on dictionary codes (Phase 2: fast path)
/// Returns a hash map: code -> aggregate values
pub fn groupby_dictionary_codes<F>(
    codes: &Arc<dyn Array>,
    dictionary: &[Value],
    aggregate_fn: F,
) -> Result<FxHashMap<u32, Vec<Value>>>
where
    F: Fn(&Value) -> Value,
{
    let mut groups: FxHashMap<u32, Vec<Value>> = FxHashMap::default();
    let row_count = codes.len();
    
    // Extract values from other columns (passed via aggregate_fn)
    // For now, this is a placeholder - actual implementation would need access to other columns
    match codes.data_type() {
        DataType::UInt16 => {
            let arr = codes.as_any().downcast_ref::<UInt16Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to UInt16Array"))?;
            for i in 0..row_count {
                if !arr.is_null(i) {
                    let code = arr.value(i) as u32;
                    // In real implementation, aggregate_fn would extract value from other columns
                    let agg_val = aggregate_fn(&Value::Int64(i as i64)); // Placeholder
                    groups.entry(code).or_insert_with(Vec::new).push(agg_val);
                }
            }
        }
        DataType::UInt32 => {
            let arr = codes.as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to UInt32Array"))?;
            for i in 0..row_count {
                if !arr.is_null(i) {
                    let code = arr.value(i);
                    let agg_val = aggregate_fn(&Value::Int64(i as i64)); // Placeholder
                    groups.entry(code).or_insert_with(Vec::new).push(agg_val);
                }
            }
        }
        _ => {
            anyhow::bail!("Dictionary codes must be UInt16 or UInt32");
        }
    }
    
    Ok(groups)
}

/// Check if a fragment is dictionary-encoded
pub fn is_dictionary_encoded(fragment: &ColumnFragment) -> bool {
    matches!(fragment.metadata.compression, crate::storage::fragment::CompressionType::Dictionary)
        && fragment.dictionary.is_some()
}

/// Get dictionary-encoded codes array from fragment
pub fn get_dictionary_codes(fragment: &ColumnFragment) -> Option<Arc<dyn Array>> {
    if is_dictionary_encoded(fragment) {
        fragment.array.clone()
    } else {
        None
    }
}

/// Get dictionary from fragment
pub fn get_dictionary(fragment: &ColumnFragment) -> Option<&[Value]> {
    fragment.dictionary.as_deref()
}

