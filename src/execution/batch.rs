use crate::storage::columnar::ColumnarBatch;
use crate::storage::fragment::ColumnFragment;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use std::collections::HashMap;

/// Execution batch - optimized batch for execution pipeline
/// Uses SIMD-friendly layouts and zero-copy where possible
#[derive(Clone)]
pub struct ExecutionBatch {
    /// Columnar batch
    pub batch: ColumnarBatch,
    
    /// Selection vector (bitmap of valid rows)
    pub selection: bitvec::prelude::BitVec,
    
    /// Row count (after selection)
    pub row_count: usize,
    
    /// Column fragments metadata (for dictionary encoding lookup)
    /// Maps column name -> fragment (optional, only for dictionary-encoded columns)
    pub column_fragments: HashMap<String, Arc<ColumnFragment>>,
}

impl ExecutionBatch {
    pub fn new(batch: ColumnarBatch) -> Self {
        let row_count = batch.row_count;
        let selection = bitvec![1; row_count];
        
        Self {
            batch,
            selection,
            row_count,
            column_fragments: HashMap::new(),
        }
    }
    
    /// Create batch with column fragment metadata (for dictionary encoding)
    pub fn with_fragments(batch: ColumnarBatch, column_fragments: HashMap<String, Arc<ColumnFragment>>) -> Self {
        let row_count = batch.row_count;
        let selection = bitvec![1; row_count];
        
        Self {
            batch,
            selection,
            row_count,
            column_fragments,
        }
    }
    
    /// Apply selection vector (filter rows)
    pub fn apply_selection(&mut self, new_selection: &bitvec::prelude::BitVec) {
        self.selection = new_selection.clone();
        self.row_count = self.selection.count_ones();
    }
    
    /// Get selected row count
    pub fn selected_count(&self) -> usize {
        self.row_count
    }
    
    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
    
    /// Slice batch (zero-copy)
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let batch = self.batch.slice(offset, length);
        let selection_slice = &self.selection[offset..offset + length];
        let mut new_selection = bitvec::prelude::BitVec::with_capacity(length);
        new_selection.extend_from_bitslice(selection_slice);
        let row_count = new_selection.count_ones();
        
        Self {
            batch,
            selection: new_selection,
            row_count,
            column_fragments: self.column_fragments.clone(), // Fragments are shared
        }
    }
    
    /// Get fragment for a column (for dictionary encoding lookup)
    pub fn get_column_fragment(&self, column_name: &str) -> Option<&Arc<ColumnFragment>> {
        self.column_fragments.get(column_name)
    }
}

/// Batch iterator - produces batches of rows
pub trait BatchIterator: Send {
    /// Get next batch
    fn next(&mut self) -> Result<Option<ExecutionBatch>, anyhow::Error>;
    
    /// Get schema
    fn schema(&self) -> SchemaRef;
}

