/// Sparse index builder for time-series data
/// Builds a sparse index by sampling rows at regular intervals
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use serde::{Serialize, Deserialize};
use std::cmp::Ordering;

/// Entry in a sparse index
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SparseIndexEntry {
    pub key: i64,
    pub row: usize,
}

impl PartialOrd for SparseIndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl Ord for SparseIndexEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// Builder for sparse indexes
/// Samples rows at regular intervals to create a sparse index
pub struct SparseIndexBuilder {
    pub entries: Vec<SparseIndexEntry>,
}

impl SparseIndexBuilder {
    /// Create a new sparse index builder
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Build sparse index from a RecordBatch
    /// Samples every `sample`-th row from the specified column
    /// 
    /// # Arguments
    /// * `batch` - The RecordBatch to index
    /// * `col` - Column name to index
    /// * `sample` - Sampling rate (every Nth row)
    pub fn build(&mut self, batch: &RecordBatch, col: &str, sample: usize) -> Result<(), anyhow::Error> {
        let col_idx = batch.schema().index_of(col)
            .map_err(|_| anyhow::anyhow!("Column '{}' not found in batch schema", col))?;
        
        let arr = batch.column(col_idx);
        let col_array = arr.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Column '{}' is not Int64", col))?;
        
        // Sample rows at regular intervals
        for i in (0..col_array.len()).step_by(sample.max(1)) {
            if !col_array.is_null(i) {
                self.entries.push(SparseIndexEntry {
                    key: col_array.value(i),
                    row: i,
                });
            }
        }
        
        // Sort entries by key for efficient binary search
        self.entries.sort();
        
        Ok(())
    }
    
    /// Build sparse index with default sampling rate (every 100th row)
    pub fn build_default(&mut self, batch: &RecordBatch, col: &str) -> Result<(), anyhow::Error> {
        self.build(batch, col, 100)
    }
    
    /// Get the number of entries in the index
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    /// Finish building and create a SparseIndexReader
    pub fn finish(self) -> crate::index::sparse::reader::SparseIndexReader {
        crate::index::sparse::reader::SparseIndexReader {
            entries: self.entries,
        }
    }
}

impl Default for SparseIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

