/// Bitmap index: maps each distinct value in a column to a compressed bitset
/// of row-ids where that value appears.
/// 
/// This enables efficient multiway joins via bitset intersection.
use crate::storage::bitset::Bitset;
use crate::storage::fragment::Value;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Maps each distinct value in a column to a compressed bitset
/// of row-ids where that value appears.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitmapIndex {
    /// Value -> index into bitsets vector
    pub dict: HashMap<Value, usize>,
    /// Index -> bitset of rows where that value appears
    pub bitsets: Vec<Bitset>,
    /// Column name this index is for
    pub column_name: String,
    /// Table name
    pub table_name: String,
    /// Total number of rows indexed
    pub row_count: usize,
}

impl BitmapIndex {
    /// Create a new empty bitmap index
    pub fn new(table_name: String, column_name: String) -> Self {
        Self {
            dict: HashMap::new(),
            bitsets: Vec::new(),
            column_name,
            table_name,
            row_count: 0,
        }
    }

    /// Add a value at a specific row ID
    pub fn add_value(&mut self, value: Value, row_id: usize) {
        let idx = *self.dict.entry(value.clone())
            .or_insert_with(|| {
                self.bitsets.push(Bitset::new());
                self.bitsets.len() - 1
            });
        self.bitsets[idx].set(row_id);
        self.row_count = self.row_count.max(row_id + 1);
    }

    /// Get the bitset for a specific value
    pub fn get_bitset(&self, value: &Value) -> Option<&Bitset> {
        self.dict.get(value).and_then(|&idx| self.bitsets.get(idx))
    }

    /// Get the bitset for a specific value (mutable)
    pub fn get_bitset_mut(&mut self, value: &Value) -> Option<&mut Bitset> {
        if let Some(&idx) = self.dict.get(value) {
            self.bitsets.get_mut(idx)
        } else {
            None
        }
    }

    /// Get all distinct values in the index
    pub fn distinct_values(&self) -> Vec<&Value> {
        self.dict.keys().collect()
    }

    /// Get the number of distinct values
    pub fn distinct_count(&self) -> usize {
        self.dict.len()
    }

    /// Estimate selectivity for a value (fraction of rows that match)
    pub fn estimate_selectivity(&self, value: &Value) -> f64 {
        if self.row_count == 0 {
            return 0.0;
        }
        if let Some(bitset) = self.get_bitset(value) {
            bitset.popcount() as f64 / self.row_count as f64
        } else {
            0.0
        }
    }

    /// Check if index should be used for a join (heuristic: domain size < threshold)
    pub fn should_use_for_join(&self, threshold: usize) -> bool {
        self.distinct_count() < threshold
    }

    /// Build index from a column array
    pub fn build_from_array(
        table_name: String,
        column_name: String,
        values: &[Value],
    ) -> Self {
        let mut index = Self::new(table_name, column_name);
        for (row_id, value) in values.iter().enumerate() {
            index.add_value(value.clone(), row_id);
        }
        index
    }

    /// Intersect with another bitmap index (for joins)
    /// Returns a mapping of matching values to their intersection bitsets
    pub fn intersect_with(
        &self,
        other: &BitmapIndex,
    ) -> HashMap<Value, Bitset> {
        let mut result = HashMap::new();
        
        for (value, &idx) in &self.dict {
            if let Some(&other_idx) = other.dict.get(value) {
                let intersection = self.bitsets[idx].intersect(&other.bitsets[other_idx]);
                if intersection.any() {
                    result.insert(value.clone(), intersection);
                }
            }
        }
        
        result
    }
}

/// Inverted list: alternative representation for value -> row-id list
/// Used when bitset representation is not efficient (very sparse data)
#[derive(Clone, Debug)]
pub struct InvertedList {
    /// Value -> list of row IDs
    pub map: HashMap<Value, Vec<usize>>,
    /// Column name
    pub column_name: String,
    /// Table name
    pub table_name: String,
}

impl InvertedList {
    /// Create a new empty inverted list
    pub fn new(table_name: String, column_name: String) -> Self {
        Self {
            map: HashMap::new(),
            column_name,
            table_name,
        }
    }

    /// Add a value at a specific row ID
    pub fn add(&mut self, value: Value, row_id: usize) {
        self.map.entry(value).or_default().push(row_id);
    }

    /// Get row IDs for a specific value
    pub fn get(&self, value: &Value) -> Option<&Vec<usize>> {
        self.map.get(value)
    }

    /// Convert to bitmap index (if beneficial)
    pub fn to_bitmap_index(&self, max_row: usize) -> BitmapIndex {
        let mut index = BitmapIndex::new(
            self.table_name.clone(),
            self.column_name.clone(),
        );
        
        for (value, row_ids) in &self.map {
            for &row_id in row_ids {
                if row_id < max_row {
                    index.add_value(value.clone(), row_id);
                }
            }
        }
        
        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_index_basic() {
        let mut index = BitmapIndex::new("table".to_string(), "col".to_string());
        
        index.add_value(Value::Int64(100), 0);
        index.add_value(Value::Int64(100), 1);
        index.add_value(Value::Int64(200), 2);
        
        let bs = index.get_bitset(&Value::Int64(100)).unwrap();
        assert!(bs.get(0));
        assert!(bs.get(1));
        assert!(!bs.get(2));
        
        assert_eq!(index.distinct_count(), 2);
    }

    #[test]
    fn test_bitmap_index_intersect() {
        let mut index1 = BitmapIndex::new("t1".to_string(), "col".to_string());
        let mut index2 = BitmapIndex::new("t2".to_string(), "col".to_string());
        
        // index1: value 100 at rows 0,1; value 200 at row 2
        index1.add_value(Value::Int64(100), 0);
        index1.add_value(Value::Int64(100), 1);
        index1.add_value(Value::Int64(200), 2);
        
        // index2: value 100 at row 0; value 200 at row 1
        index2.add_value(Value::Int64(100), 0);
        index2.add_value(Value::Int64(200), 1);
        
        let intersections = index1.intersect_with(&index2);
        
        // The intersection only includes values where row IDs overlap
        // For value 100: index1 has rows 0,1; index2 has row 0 -> intersection has row 0 (both have it) ✓
        // For value 200: index1 has row 2; index2 has row 1 -> intersection is empty (no common row IDs) ✗
        
        // So we should have exactly 1 result (value 100, since row 0 matches)
        assert_eq!(intersections.len(), 1);
        assert!(intersections.contains_key(&Value::Int64(100)));
        assert!(!intersections.contains_key(&Value::Int64(200))); // Value 200 has no overlapping row IDs
        
        // Verify value 100 intersection - row 0 should be in the intersection
        let inter_100 = intersections.get(&Value::Int64(100)).unwrap();
        assert!(inter_100.get(0)); // Row 0 is in both indexes
        assert!(!inter_100.get(1)); // Row 1 is only in index1
    }
}

