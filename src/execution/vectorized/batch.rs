/// VectorBatch: Arrow-backed vector batch for async, vectorized execution
/// 
/// This is the new batch model for Phase 1, designed for:
/// - Zero-copy operations where possible
/// - Selection bitmaps for filtering
/// - Metadata for operator fusion
/// - Arrow-native arrays for vectorization
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use crate::error::{EngineResult, EngineError};

/// Vector batch - Arrow-backed with selection bitmap
/// 
/// This is the core data structure for the new vectorized execution model.
/// It's similar to ExecutionBatch but designed for async, zero-copy operations.
#[derive(Clone)]
pub struct VectorBatch {
    /// Arrow arrays (one per column)
    pub columns: Vec<Arc<dyn Array>>,
    
    /// Arrow arrays (one per column) - alias for columns
    #[deprecated(note = "Use columns instead")]
    pub arrays: Vec<Arc<dyn Array>>,
    
    /// Schema for this batch
    pub schema: SchemaRef,
    
    /// Selection bitmap (optional - if None, all rows are selected)
    /// When present, indicates which rows are valid/selected
    pub selection: Option<BitVec>,
    
    /// Row count (number of selected rows if selection is Some, else arrays[0].len())
    pub row_count: usize,
    
    /// Metadata for operator fusion and optimization
    pub metadata: BatchMetadata,
}

/// Metadata attached to batches for operator fusion and optimization
#[derive(Clone, Debug, Default)]
pub struct BatchMetadata {
    /// Whether this batch is sorted (and by which columns)
    pub is_sorted: Option<Vec<usize>>,
    
    /// Whether this batch has been filtered (for predicate pushdown tracking)
    pub is_filtered: bool,
    
    /// Estimated cardinality (for cost-based optimizations)
    pub estimated_cardinality: Option<usize>,
    
    /// Custom metadata map for operator-specific information
    pub custom: std::collections::HashMap<String, String>,
}

impl VectorBatch {
    /// Create a new VectorBatch from Arrow arrays
    pub fn new(arrays: Vec<Arc<dyn Array>>, schema: SchemaRef) -> Self {
        let row_count = arrays.first().map(|a| a.len()).unwrap_or(0);
        let columns = arrays.clone();
        Self {
            arrays: arrays.clone(),
            columns,
            schema,
            selection: None,
            row_count,
            metadata: BatchMetadata::default(),
        }
    }
    
    /// Create a VectorBatch with a selection bitmap
    pub fn with_selection(
        arrays: Vec<Arc<dyn Array>>,
        schema: SchemaRef,
        selection: BitVec,
    ) -> Self {
        let row_count = selection.count_ones();
        let columns = arrays.clone();
        Self {
            arrays: arrays.clone(),
            columns,
            schema,
            selection: Some(selection),
            row_count,
            metadata: BatchMetadata::default(),
        }
    }
    
    /// Get a column array by index
    pub fn column(&self, idx: usize) -> EngineResult<Arc<dyn Array>> {
        self.columns.get(idx)
            .ok_or_else(|| EngineError::execution(format!(
                "Column index {} out of bounds (batch has {} columns)",
                idx,
                self.columns.len()
            )))
            .cloned()
    }
    
    /// Get the selection bitmap (creating one if None)
    pub fn selection(&self) -> BitVec {
        self.selection.clone().unwrap_or_else(|| {
            bitvec![1; self.row_count]
        })
    }
    
    /// Apply a selection bitmap to this batch (zero-copy where possible)
    pub fn apply_selection(&self, new_selection: BitVec) -> Self {
        // Combine with existing selection if present
        let combined_selection = if let Some(ref existing) = self.selection {
            existing.clone() & new_selection
        } else {
            new_selection
        };
        
        let row_count = combined_selection.count_ones();
        Self {
            columns: self.columns.clone(), // Zero-copy: columns are Arc
            arrays: self.columns.clone(), // Keep arrays for backward compatibility
            schema: self.schema.clone(),
            selection: Some(combined_selection),
            row_count,
            metadata: self.metadata.clone(),
        }
    }
    
    /// Slice this batch (zero-copy)
    pub fn slice(&self, offset: usize, length: usize) -> EngineResult<Self> {
        if offset + length > self.row_count {
            return Err(EngineError::execution(format!(
                "Slice out of bounds: offset {} + length {} > row_count {}",
                offset, length, self.row_count
            )));
        }
        
        // Slice arrays (zero-copy)
        let sliced_arrays: Vec<Arc<dyn Array>> = self.columns.iter()
            .map(|arr| arr.slice(offset, length))
            .collect();
        
        // Slice selection bitmap if present
        let sliced_selection = self.selection.as_ref().map(|sel| {
            sel[offset..offset + length].to_bitvec()
        });
        
        let columns = sliced_arrays.clone();
        Ok(Self {
            columns,
            arrays: sliced_arrays,
            schema: self.schema.clone(),
            selection: sliced_selection,
            row_count: length,
            metadata: self.metadata.clone(),
        })
    }
    
    /// Convert to Arrow RecordBatch
    pub fn to_record_batch(&self) -> EngineResult<arrow::record_batch::RecordBatch> {
        use arrow::record_batch::RecordBatch;
        
        // Apply selection if present
        let columns = if let Some(selection) = &self.selection {
            // For now, return columns as-is
            // TODO: Apply selection bitmap properly to filter arrays
            self.columns.clone()
        } else {
            self.columns.clone()
        };
        
        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| EngineError::execution(format!(
                "Failed to create RecordBatch: {}", e
            )))
    }
    
    /// Create from Arrow RecordBatch
    pub fn from_record_batch(batch: &arrow::record_batch::RecordBatch) -> EngineResult<Self> {
        let columns = batch.columns().to_vec();
        Ok(Self {
            columns: columns.clone(),
            arrays: columns,
            schema: batch.schema(),
            row_count: batch.num_rows(),
            selection: None,
            metadata: BatchMetadata::default(),
        })
    }
    
    /// Convert to ExecutionBatch (for backward compatibility during migration)
    pub fn to_execution_batch(&self) -> EngineResult<crate::execution::batch::ExecutionBatch> {
        use crate::storage::columnar::ColumnarBatch;
        
        let columnar_batch = ColumnarBatch {
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            row_count: self.row_count,
        };
        
        let mut exec_batch = crate::execution::batch::ExecutionBatch::new(columnar_batch);
        
        // Apply selection if present
        if let Some(ref sel) = self.selection {
            exec_batch.selection = sel.clone();
            exec_batch.row_count = sel.count_ones();
        }
        
        Ok(exec_batch)
    }
}

/// Convert ExecutionBatch to VectorBatch (for migration)
impl From<&crate::execution::batch::ExecutionBatch> for VectorBatch {
    fn from(batch: &crate::execution::batch::ExecutionBatch) -> Self {
        let selection = if batch.selection.count_ones() == batch.batch.row_count {
            None // All rows selected, no need for bitmap
        } else {
            Some(batch.selection.clone())
        };
        
        let columns = batch.batch.columns.clone();
        Self {
            columns: columns.clone(),
            arrays: columns,
            schema: batch.batch.schema.clone(),
            selection,
            row_count: batch.row_count,
            metadata: BatchMetadata::default(),
        }
    }
}

