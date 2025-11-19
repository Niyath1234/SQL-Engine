use crate::storage::fragment::{ColumnFragment, Value};
use bitvec::prelude::*;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Index for fast lookups on column fragments
pub trait ColumnIndex {
    /// Check if a value might exist (using bloom filter)
    fn might_contain(&self, value: &Value) -> bool;
    
    /// Get bitmap of rows matching a predicate
    fn get_matching_rows(&self, predicate: &Predicate) -> BitVec;
}

/// Predicate for filtering
#[derive(Clone, Debug)]
pub enum Predicate {
    Equals(Value),
    NotEquals(Value),
    LessThan(Value),
    LessThanOrEqual(Value),
    GreaterThan(Value),
    GreaterThanOrEqual(Value),
    Between(Value, Value),
    In(Vec<Value>),
    IsNull,
    IsNotNull,
}

/// Bloom filter index (simplified implementation)
#[derive(Clone)]
pub struct BloomIndex {
    bits: Vec<u8>,
    capacity: usize,
}

impl BloomIndex {
    pub fn new(capacity: usize, _error_rate: f64) -> Self {
        // Simple bloom filter: use 8 bits per element
        let bit_size = capacity * 8;
        let byte_size = (bit_size + 7) / 8;
        Self {
            bits: vec![0; byte_size],
            capacity,
        }
    }
    
    pub fn insert(&mut self, value: &Value) {
        let hash = self.hash_value(value);
        let index = (hash as usize) % (self.bits.len() * 8);
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }
}

impl ColumnIndex for BloomIndex {
    fn might_contain(&self, value: &Value) -> bool {
        let hash = self.hash_value(value);
        let index = (hash as usize) % (self.bits.len() * 8);
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        (self.bits[byte_idx] & (1 << bit_idx)) != 0
    }
    
    fn get_matching_rows(&self, _predicate: &Predicate) -> BitVec {
        // Bloom filter can't give exact matches, only "might contain"
        bitvec![1; 0]
    }
}

impl BloomIndex {
    fn hash_value(&self, value: &Value) -> u64 {
        use fxhash::FxHasher;
        
        let mut hasher = FxHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }
}

/// Sorted segment index (for sorted fragments)
#[derive(Clone)]
pub struct SortedIndex {
    segments: Vec<SortedSegment>,
}

#[derive(Clone)]
struct SortedSegment {
    min_value: Value,
    max_value: Value,
    start_row: usize,
    end_row: usize,
}

impl SortedIndex {
    pub fn new(fragment: &ColumnFragment) -> Self {
        if !fragment.is_sorted || fragment.len() == 0 {
            return Self {
                segments: vec![],
            };
        }
        
        // Build segments: divide fragment into chunks
        let segment_size = (fragment.len() / 100).max(1000).min(fragment.len());
        let mut segments = Vec::new();
        
        for start in (0..fragment.len()).step_by(segment_size) {
            let end = (start + segment_size).min(fragment.len());
            if let (Some(min_val), Some(max_val)) = (
                Self::get_value_at(fragment, start),
                Self::get_value_at(fragment, end.saturating_sub(1)),
            ) {
                segments.push(SortedSegment {
                    min_value: min_val,
                    max_value: max_val,
                    start_row: start,
                    end_row: end,
                });
            }
        }
        
        Self { segments }
    }
    
    fn get_value_at(fragment: &ColumnFragment, idx: usize) -> Option<Value> {
        if idx >= fragment.len() {
            return None;
        }
        
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return Some(Value::Null);
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|arr| Value::Int64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .map(|arr| Value::Float64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Utf8 => {
                array.as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .map(|arr| Value::String(arr.value(idx).to_string()))
            }
            _ => None,
        }
    }
    
    /// Binary search for value range
    pub fn find_range(&self, predicate: &Predicate) -> Option<(usize, usize)> {
        // Find relevant segments
        let relevant_segments: Vec<&SortedSegment> = self.segments.iter()
            .filter(|seg| {
                match predicate {
                    Predicate::Equals(val) => {
                        Self::value_in_range(val, &seg.min_value, &seg.max_value)
                    }
                    Predicate::LessThan(val) => {
                        seg.min_value < *val
                    }
                    Predicate::LessThanOrEqual(val) => {
                        seg.min_value <= *val
                    }
                    Predicate::GreaterThan(val) => {
                        seg.max_value > *val
                    }
                    Predicate::GreaterThanOrEqual(val) => {
                        seg.max_value >= *val
                    }
                    Predicate::Between(low, high) => {
                        !(seg.max_value < *low || seg.min_value > *high)
                    }
                    _ => true,
                }
            })
            .collect();
        
        if relevant_segments.is_empty() {
            return None;
        }
        
        let start = relevant_segments.first().unwrap().start_row;
        let end = relevant_segments.last().unwrap().end_row;
        
        Some((start, end))
    }
    
    fn value_in_range(value: &Value, min: &Value, max: &Value) -> bool {
        value >= min && value <= max
    }
}

impl ColumnIndex for SortedIndex {
    fn might_contain(&self, value: &Value) -> bool {
        self.find_range(&Predicate::Equals(value.clone())).is_some()
    }
    
    fn get_matching_rows(&self, predicate: &Predicate) -> BitVec {
        if let Some((start, end)) = self.find_range(predicate) {
            let mut result = bitvec![0; end];
            result[start..end].fill(true);
            result
        } else {
            bitvec![0; 0]
        }
    }
}

/// Hash index for fast equality lookups
#[derive(Clone)]
pub struct HashIndex {
    /// Map from value hash to row positions
    index: HashMap<u64, Vec<usize>>,
    row_count: usize,
}

impl HashIndex {
    pub fn build(fragment: &ColumnFragment) -> Self {
        let mut index = HashMap::new();
        
        for i in 0..fragment.len() {
            if let Some(value) = Self::extract_value(fragment, i) {
                let hash = Self::hash_value(&value);
                index.entry(hash).or_insert_with(Vec::new).push(i);
            }
        }
        
        Self {
            index,
            row_count: fragment.len(),
        }
    }
    
    fn extract_value(fragment: &ColumnFragment, idx: usize) -> Option<Value> {
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return Some(Value::Null);
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|arr| Value::Int64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .map(|arr| Value::Float64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Utf8 => {
                array.as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .map(|arr| Value::String(arr.value(idx).to_string()))
            }
            _ => None,
        }
    }
    
    fn hash_value(value: &Value) -> u64 {
        use fxhash::FxHasher;
        use std::hash::Hasher;
        let mut hasher = FxHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }
}

impl ColumnIndex for HashIndex {
    fn might_contain(&self, value: &Value) -> bool {
        let hash = HashIndex::hash_value(value);
        self.index.contains_key(&hash)
    }
    
    fn get_matching_rows(&self, predicate: &Predicate) -> BitVec {
        match predicate {
            Predicate::Equals(value) => {
                let hash = HashIndex::hash_value(value);
                let mut result = bitvec![0; self.row_count];
                if let Some(positions) = self.index.get(&hash) {
                    for &pos in positions {
                        if pos < self.row_count {
                            result.set(pos, true);
                        }
                    }
                }
                result
            }
            _ => {
                // Hash index only supports equality
                bitvec![0; self.row_count]
            }
        }
    }
}

/// B-tree index for range queries
#[derive(Clone)]
pub struct BTreeIndex {
    /// Tree nodes (simplified: just store sorted key-value pairs)
    nodes: Vec<BTreeNode>,
    row_count: usize,
}

#[derive(Clone)]
struct BTreeNode {
    keys: Vec<Value>,
    positions: Vec<usize>,
    is_leaf: bool,
}

impl BTreeIndex {
    pub fn build(fragment: &ColumnFragment, _order: usize) -> Self {
        // Simplified B-tree: just create a sorted list
        let mut key_positions: Vec<(Value, usize)> = Vec::new();
        
        for i in 0..fragment.len() {
            if let Some(value) = Self::extract_value(fragment, i) {
                key_positions.push((value, i));
            }
        }
        
        // Sort by value
        key_positions.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Build simple node structure
        let node = BTreeNode {
            keys: key_positions.iter().map(|(k, _)| k.clone()).collect(),
            positions: key_positions.iter().map(|(_, p)| *p).collect(),
            is_leaf: true,
        };
        
        Self {
            nodes: vec![node],
            row_count: fragment.len(),
        }
    }
    
    fn extract_value(fragment: &ColumnFragment, idx: usize) -> Option<Value> {
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return Some(Value::Null);
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|arr| Value::Int64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .map(|arr| Value::Float64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Utf8 => {
                array.as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .map(|arr| Value::String(arr.value(idx).to_string()))
            }
            _ => None,
        }
    }
    
    fn find_range(&self, predicate: &Predicate) -> Option<(usize, usize)> {
        if self.nodes.is_empty() {
            return None;
        }
        
        let node = &self.nodes[0];
        
        match predicate {
            Predicate::Equals(val) => {
                node.keys.binary_search_by(|k| k.partial_cmp(val).unwrap_or(std::cmp::Ordering::Equal))
                    .ok()
                    .map(|idx| (node.positions[idx], node.positions[idx] + 1))
            }
            Predicate::LessThan(val) => {
                let idx = node.keys.binary_search_by(|k| k.partial_cmp(val).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or_else(|i| i);
                if idx > 0 {
                    Some((0, node.positions[idx - 1] + 1))
                } else {
                    None
                }
            }
            Predicate::GreaterThan(val) => {
                let idx = node.keys.binary_search_by(|k| k.partial_cmp(val).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or_else(|i| i);
                if idx < node.positions.len() {
                    Some((node.positions[idx], self.row_count))
                } else {
                    None
                }
            }
            Predicate::Between(low, high) => {
                let low_idx = node.keys.binary_search_by(|k| k.partial_cmp(low).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or_else(|i| i);
                let high_idx = node.keys.binary_search_by(|k| k.partial_cmp(high).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or_else(|i| i);
                if low_idx < high_idx {
                    Some((node.positions[low_idx], node.positions[high_idx.min(node.positions.len() - 1)] + 1))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl ColumnIndex for BTreeIndex {
    fn might_contain(&self, value: &Value) -> bool {
        self.find_range(&Predicate::Equals(value.clone())).is_some()
    }
    
    fn get_matching_rows(&self, predicate: &Predicate) -> BitVec {
        if let Some((start, end)) = self.find_range(predicate) {
            let mut result = bitvec![0; self.row_count];
            for i in start..end.min(self.row_count) {
                result.set(i, true);
            }
            result
        } else {
            bitvec![0; self.row_count]
        }
    }
}

// Hash implementation moved to fragment.rs to avoid conflicts

