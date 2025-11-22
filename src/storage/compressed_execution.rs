/// Compressed-aware execution kernels
/// Execute filters and aggregates directly on compressed data without decompression
use crate::storage::fragment::{ColumnFragment, CompressionType, Value};
use arrow::array::*;
use arrow::datatypes::*;
use bitvec::prelude::*;
use std::sync::Arc;
use anyhow::Result;

/// RLE (Run-Length Encoded) representation
#[derive(Clone, Debug)]
pub struct RLEData {
    /// Values (one per run)
    pub values: Vec<Value>,
    /// Run lengths (number of times each value repeats)
    pub lengths: Vec<usize>,
    /// Total number of rows
    pub total_rows: usize,
}

/// Dictionary-encoded representation
#[derive(Clone, Debug)]
pub struct DictionaryData {
    /// Dictionary: index -> value
    pub dictionary: Vec<Value>,
    /// Encoded indices (one per row)
    pub indices: Vec<u32>,
}

/// Compressed-aware filter: filter RLE data without decompressing
pub fn filter_rle(
    rle: &RLEData,
    predicate: &CompressedFilterPredicate,
) -> Result<BitVec> {
    let mut result = bitvec![0; rle.total_rows];
    let mut row_idx = 0;
    
    for (value, &length) in rle.values.iter().zip(&rle.lengths) {
        let matches = match predicate {
            CompressedFilterPredicate::Equals(target) => value == target,
            CompressedFilterPredicate::GreaterThan(target) => value > target,
            CompressedFilterPredicate::LessThan(target) => value < target,
            CompressedFilterPredicate::GreaterEqual(target) => value >= target,
            CompressedFilterPredicate::LessEqual(target) => value <= target,
            CompressedFilterPredicate::NotEqual(target) => value != target,
        };
        
        // Set all rows in this run at once
        if matches {
            for i in 0..length {
                if row_idx + i < rle.total_rows {
                    result.set(row_idx + i, true);
                }
            }
        }
        
        row_idx += length;
    }
    
    Ok(result)
}

/// Compressed-aware filter: filter Dictionary-encoded data
pub fn filter_dictionary(
    dict: &DictionaryData,
    predicate: &CompressedFilterPredicate,
) -> Result<BitVec> {
    let mut result = bitvec![0; dict.indices.len()];
    
    // First, find which dictionary indices match the predicate
    let mut matching_indices = bitvec![0; dict.dictionary.len()];
    for (idx, value) in dict.dictionary.iter().enumerate() {
        let matches = match predicate {
            CompressedFilterPredicate::Equals(target) => value == target,
            CompressedFilterPredicate::GreaterThan(target) => value > target,
            CompressedFilterPredicate::LessThan(target) => value < target,
            CompressedFilterPredicate::GreaterEqual(target) => value >= target,
            CompressedFilterPredicate::LessEqual(target) => value <= target,
            CompressedFilterPredicate::NotEqual(target) => value != target,
        };
        if matches {
            matching_indices.set(idx, true);
        }
    }
    
    // Then, mark rows whose dictionary index matches
    for (row_idx, &dict_idx) in dict.indices.iter().enumerate() {
        if dict_idx < matching_indices.len() as u32 && matching_indices[dict_idx as usize] {
            result.set(row_idx, true);
        }
    }
    
    Ok(result)
}

/// Compressed-aware aggregate: compute SUM on RLE data
pub fn aggregate_sum_rle(rle: &RLEData) -> Result<f64> {
    let mut sum = 0.0;
    
    for (value, &length) in rle.values.iter().zip(&rle.lengths) {
        let val = match value {
            Value::Int64(v) => *v as f64,
            Value::Int32(v) => *v as f64,
            Value::Float64(v) => *v,
            Value::Float32(v) => *v as f64,
            _ => 0.0,
        };
        sum += val * length as f64;
    }
    
    Ok(sum)
}

/// Compressed-aware aggregate: compute COUNT on RLE data
pub fn aggregate_count_rle(rle: &RLEData) -> Result<usize> {
    Ok(rle.total_rows)
}

/// Compressed-aware aggregate: compute AVG on RLE data
pub fn aggregate_avg_rle(rle: &RLEData) -> Result<f64> {
    let sum = aggregate_sum_rle(rle)?;
    Ok(sum / rle.total_rows as f64)
}

/// Compressed-aware aggregate: compute SUM on Dictionary-encoded data
pub fn aggregate_sum_dictionary(dict: &DictionaryData) -> Result<f64> {
    let mut sum = 0.0;
    
    for &dict_idx in &dict.indices {
        if dict_idx < dict.dictionary.len() as u32 {
            let value = &dict.dictionary[dict_idx as usize];
            let val = match value {
                Value::Int64(v) => *v as f64,
                Value::Int32(v) => *v as f64,
                Value::Float64(v) => *v,
                Value::Float32(v) => *v as f64,
                _ => 0.0,
            };
            sum += val;
        }
    }
    
    Ok(sum)
}

/// Compressed-aware aggregate: compute COUNT on Dictionary-encoded data
pub fn aggregate_count_dictionary(dict: &DictionaryData) -> Result<usize> {
    Ok(dict.indices.len())
}

/// Compressed-aware aggregate: compute AVG on Dictionary-encoded data
pub fn aggregate_avg_dictionary(dict: &DictionaryData) -> Result<f64> {
    let sum = aggregate_sum_dictionary(dict)?;
    Ok(sum / dict.indices.len() as f64)
}

/// Extract RLE data from a fragment (if it's RLE compressed)
pub fn extract_rle(fragment: &ColumnFragment) -> Option<RLEData> {
    if !matches!(fragment.metadata.compression, CompressionType::RLE) {
        return None;
    }
    
    // For now, we'll need to decode from the array
    // In a full implementation, we'd store RLE directly
    // For now, return None and fall back to decompressed path
    None
}

/// Extract Dictionary data from a fragment (if it's Dictionary compressed)
pub fn extract_dictionary(fragment: &ColumnFragment) -> Option<DictionaryData> {
    if !matches!(fragment.metadata.compression, CompressionType::Dictionary) {
        return None;
    }
    
    // For now, we'll need to decode from the array
    // In a full implementation, we'd store Dictionary directly
    // For now, return None and fall back to decompressed path
    None
}

/// Filter predicate for compressed execution
#[derive(Clone, Debug)]
pub enum CompressedFilterPredicate {
    Equals(Value),
    GreaterThan(Value),
    LessThan(Value),
    GreaterEqual(Value),
    LessEqual(Value),
    NotEqual(Value),
}

impl CompressedFilterPredicate {
    pub fn from_value(value: Value, operator: &str) -> Self {
        match operator {
            "=" => CompressedFilterPredicate::Equals(value),
            ">" => CompressedFilterPredicate::GreaterThan(value),
            "<" => CompressedFilterPredicate::LessThan(value),
            ">=" => CompressedFilterPredicate::GreaterEqual(value),
            "<=" => CompressedFilterPredicate::LessEqual(value),
            "!=" | "<>" => CompressedFilterPredicate::NotEqual(value),
            _ => CompressedFilterPredicate::Equals(value),
        }
    }
}

