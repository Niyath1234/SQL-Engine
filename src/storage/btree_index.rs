/// B-tree index for fast range scans and point lookups
/// Provides O(log n) lookup and range scan performance
use crate::storage::fragment::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// B-tree index entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BTreeIndexEntry {
    /// The indexed value
    pub value: Value,
    /// Row positions where this value appears (sorted)
    pub row_positions: Vec<usize>,
}

/// B-tree index structure
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BTreeIndex {
    /// Column name this index is on
    pub column_name: String,
    /// Table name
    pub table_name: String,
    /// B-tree map: value -> row positions
    pub tree: BTreeMap<Value, Vec<usize>>,
    /// Whether the index is unique
    pub is_unique: bool,
    /// Total number of entries
    pub entry_count: usize,
}

impl BTreeIndex {
    /// Create a new B-tree index
    pub fn new(table_name: String, column_name: String, is_unique: bool) -> Self {
        Self {
            column_name,
            table_name,
            tree: BTreeMap::new(),
            is_unique,
            entry_count: 0,
        }
    }

    /// Insert a value with its row position
    pub fn insert(&mut self, value: Value, row_position: usize) {
        self.tree
            .entry(value.clone())
            .or_insert_with(Vec::new)
            .push(row_position);
        self.entry_count += 1;
    }

    /// Build index from a sorted array of values
    pub fn build_from_values(values: &[Value], row_positions: &[usize]) -> Self {
        let mut tree = BTreeMap::new();
        for (idx, value) in values.iter().enumerate() {
            if idx < row_positions.len() {
                tree.entry(value.clone())
                    .or_insert_with(Vec::new)
                    .push(row_positions[idx]);
            }
        }
        Self {
            column_name: String::new(),
            table_name: String::new(),
            tree,
            is_unique: false,
            entry_count: values.len(),
        }
    }

    /// Point lookup: find all rows with a specific value
    pub fn lookup(&self, value: &Value) -> Option<&Vec<usize>> {
        self.tree.get(value)
    }

    /// Range scan: find all rows with values in [min, max]
    pub fn range_scan(
        &self,
        min: Option<&Value>,
        max: Option<&Value>,
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Vec<usize> {
        let mut result = Vec::new();

        let start: Box<dyn Iterator<Item = (&Value, &Vec<usize>)>> = if let Some(min_val) = min {
            if min_inclusive {
                Box::new(self.tree.range(min_val.clone()..))
            } else {
                // Need to find first value > min
                let min_val_clone = min_val.clone();
                Box::new(self.tree.range((min_val_clone.clone())..).skip_while(move |(k, _)| **k == min_val_clone))
            }
        } else {
            Box::new(self.tree.range(..))
        };

        for (key, positions) in start {
            if let Some(max_val) = max {
                let cmp = key.cmp(max_val);
                if cmp == std::cmp::Ordering::Greater || (!max_inclusive && cmp == std::cmp::Ordering::Equal) {
                    break;
                }
            }
            result.extend(positions.iter().copied());
        }

        result.sort();
        result.dedup();
        result
    }

    /// Less than: find all rows with values < value
    pub fn less_than(&self, value: &Value, inclusive: bool) -> Vec<usize> {
        self.range_scan(None, Some(value), false, inclusive)
    }

    /// Greater than: find all rows with values > value
    pub fn greater_than(&self, value: &Value, inclusive: bool) -> Vec<usize> {
        let mut result = Vec::new();
        let value_clone = value.clone();
        let start: Box<dyn Iterator<Item = (&Value, &Vec<usize>)>> = if inclusive {
            Box::new(self.tree.range(value_clone.clone()..))
        } else {
            Box::new(self.tree.range(value_clone.clone()..).skip_while(move |(k, _)| **k == value_clone))
        };

        for (_, positions) in start {
            result.extend(positions.iter().copied());
        }

        result.sort();
        result.dedup();
        result
    }

    /// Get minimum value
    pub fn min_value(&self) -> Option<&Value> {
        self.tree.keys().next()
    }

    /// Get maximum value
    pub fn max_value(&self) -> Option<&Value> {
        self.tree.keys().next_back()
    }

    /// Estimate selectivity for a range query
    pub fn estimate_selectivity(
        &self,
        min: Option<&Value>,
        max: Option<&Value>,
    ) -> f64 {
        if self.entry_count == 0 {
            return 0.0;
        }

        let total_entries = self.entry_count;
        let matching_entries = self.range_scan(min, max, true, true).len();

        if matching_entries == 0 {
            return 0.0;
        }

        matching_entries as f64 / total_entries as f64
    }
}

// Value now implements Ord, so we can use it directly with BTreeMap
// No need for compare_values helper function anymore

/// Sparse index: stores every Nth value for faster range scans on large datasets
#[derive(Clone, Debug)]
pub struct SparseIndex {
    /// Column name
    pub column_name: String,
    /// Sparse entries: value -> (first_row_position, last_row_position)
    pub sparse_entries: BTreeMap<Value, (usize, usize)>,
    /// Granularity: index every Nth value
    pub granularity: usize,
}

impl SparseIndex {
    pub fn new(column_name: String, granularity: usize) -> Self {
        Self {
            column_name,
            sparse_entries: BTreeMap::new(),
            granularity,
        }
    }

    /// Build sparse index from sorted data
    pub fn build_from_sorted(values: &[Value], row_positions: &[usize]) -> Self {
        let mut sparse_entries = BTreeMap::new();
        let granularity = (values.len() / 1000).max(1); // Index every 1000th value by default

        for (idx, value) in values.iter().enumerate() {
            if idx % granularity == 0 && idx < row_positions.len() {
                let row_pos = row_positions[idx];
                sparse_entries
                    .entry(value.clone())
                    .and_modify(|(first, last)| {
                        *last = row_pos;
                    })
                    .or_insert((row_pos, row_pos));
            }
        }

        Self {
            column_name: String::new(),
            sparse_entries,
            granularity,
        }
    }

    /// Find approximate range for a value
    pub fn find_range(&self, value: &Value) -> Option<(usize, usize)> {
        // Find the sparse entry that's <= value
        let mut candidate: Option<(&Value, &(usize, usize))> = None;
        for (k, v) in self.sparse_entries.range(..=value) {
            candidate = Some((k, v));
        }

        candidate.map(|(_, (first, last))| (*first, *last))
    }
}

