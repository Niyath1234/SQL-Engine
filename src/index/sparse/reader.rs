/// Sparse index reader for efficient range scans
/// Uses binary search to find approximate positions in sorted data
use super::builder::SparseIndexEntry;

/// Reader for sparse indexes
/// Provides efficient lookup operations using binary search
#[derive(Clone, Debug)]
pub struct SparseIndexReader {
    pub entries: Vec<SparseIndexEntry>,
}

impl SparseIndexReader {
    /// Create a new sparse index reader
    pub fn new(entries: Vec<SparseIndexEntry>) -> Self {
        Self { entries }
    }
    
    /// Find the lower bound for a given key
    /// Returns the index of the first entry with key >= target
    /// Uses binary search for O(log n) performance
    pub fn lower_bound(&self, key: i64) -> usize {
        match self.entries.binary_search_by_key(&key, |e| e.key) {
            Ok(idx) => idx,  // Exact match found
            Err(idx) => idx, // Insertion point (first entry >= key)
        }
    }
    
    /// Find the upper bound for a given key
    /// Returns the index of the first entry with key > target
    pub fn upper_bound(&self, key: i64) -> usize {
        match self.entries.binary_search_by_key(&(key + 1), |e| e.key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }
    
    /// Find the row position for a given key
    /// Returns the row index of the first entry with key >= target
    /// This is the actual row number in the original data
    pub fn find_row(&self, key: i64) -> Option<usize> {
        let idx = self.lower_bound(key);
        if idx < self.entries.len() {
            Some(self.entries[idx].row)
        } else {
            None
        }
    }
    
    /// Find the range of rows for a key range [min_key, max_key]
    /// Returns (start_row, end_row) where start_row is inclusive and end_row is exclusive
    pub fn find_range(&self, min_key: i64, max_key: i64) -> (usize, usize) {
        let start_idx = self.lower_bound(min_key);
        let end_idx = self.upper_bound(max_key);
        
        let start_row = if start_idx < self.entries.len() {
            self.entries[start_idx].row
        } else {
            // If no entry found, return a large number (scan from end)
            usize::MAX
        };
        
        let end_row = if end_idx < self.entries.len() {
            self.entries[end_idx].row
        } else {
            // If no entry found, return a large number (scan to end)
            usize::MAX
        };
        
        (start_row, end_row)
    }
    
    /// Get the number of entries in the index
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

