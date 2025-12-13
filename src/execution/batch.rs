use crate::storage::columnar::ColumnarBatch;
use crate::storage::fragment::ColumnFragment;
use crate::execution::column_identity::ColumnSchema;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use bitvec::prelude::*;
use std::collections::HashMap;
use tracing::trace;

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
    
    /// COLID: Column schema with canonical column identities
    /// This carries ColIds and name_map for column resolution
    /// If None, operators fall back to string-based resolution (backward compatibility)
    pub column_schema: Option<ColumnSchema>,
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
            column_schema: None, // COLID: Will be set by operators that create ColumnSchema
        }
    }
    
    /// Validate that ExecutionBatch schema matches ColumnarBatch structure
    /// Returns Ok(()) if valid, Err(String) with diagnostic details if mismatch
    pub fn validate_schema_alignment(&self) -> Result<(), String> {
        let batch_col_count = self.batch.columns.len();
        let schema_field_count = self.batch.schema.fields().len();
        
        // Check 1: Column count must match schema field count
        if batch_col_count != schema_field_count {
            return Err(format!(
                "Column count mismatch: batch has {} columns but schema has {} fields",
                batch_col_count, schema_field_count
            ));
        }
        
        // Check 2: If ColumnSchema exists, it must match batch structure
        if let Some(ref column_schema) = self.column_schema {
            let schema_col_count = column_schema.column_ids.len();
            if schema_col_count != batch_col_count {
                return Err(format!(
                    "ColumnSchema mismatch: ColumnSchema has {} ColIds but batch has {} columns",
                    schema_col_count, batch_col_count
                ));
            }
            
            // Check 3: External names count must match
            if column_schema.unqualified_names.len() != batch_col_count {
                return Err(format!(
                    "ExternalNames mismatch: ColumnSchema has {} external names but batch has {} columns",
                    column_schema.unqualified_names.len(), batch_col_count
                ));
            }
        }
        
        // Check 4: Each column array length must be >= row_count
        for (i, col_array) in self.batch.columns.iter().enumerate() {
            if col_array.len() < self.row_count {
                return Err(format!(
                    "Column array[{}] has length {} but row_count is {}",
                    i, col_array.len(), self.row_count
                ));
            }
        }
        
        // Check 5: Selection bitmap length must be >= row_count
        if self.selection.len() < self.row_count {
            return Err(format!(
                "Selection bitmap has length {} but row_count is {}",
                self.selection.len(), self.row_count
            ));
        }
        
        Ok(())
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
            column_schema: None, // COLID: Will be set by operators that create ColumnSchema
        }
    }
    
    /// COLID: Create batch with ColumnSchema
    pub fn with_column_schema(batch: ColumnarBatch, column_schema: ColumnSchema) -> Self {
        let row_count = batch.row_count;
        let selection = bitvec![1; row_count];
        
        Self {
            batch,
            selection,
            row_count,
            column_fragments: HashMap::new(),
            column_schema: Some(column_schema),
        }
    }
    
    /// COLID: Create batch with ColumnSchema and fragments
    pub fn with_column_schema_and_fragments(
        batch: ColumnarBatch, 
        column_schema: ColumnSchema,
        column_fragments: HashMap<String, Arc<ColumnFragment>>
    ) -> Self {
        let row_count = batch.row_count;
        let selection = bitvec![1; row_count];
        
        Self {
            batch,
            selection,
            row_count,
            column_fragments,
            column_schema: Some(column_schema),
        }
    }
    
    /// Apply selection vector (filter rows)
    pub fn apply_selection(&mut self, new_selection: &bitvec::prelude::BitVec) {
        let old_selection_len = self.selection.len();
        self.selection = new_selection.clone();
        let old_row_count = self.row_count;
        self.row_count = self.selection.count_ones();
        trace!(
            old_selection_len = old_selection_len,
            new_selection_len = self.selection.len(),
            old_row_count = old_row_count,
            new_row_count = self.row_count,
            selection_count = self.selection.count_ones(),
            "ExecutionBatch::apply_selection"
        );
        
        // Debug: Show which bits are set
        let set_bits: Vec<usize> = self.selection.iter()
            .enumerate()
            .filter_map(|(i, b)| if *b { Some(i) } else { None })
            .take(10)
            .collect();
        trace!(
            first_10_set_bits = ?set_bits,
            "ExecutionBatch::apply_selection: First 10 set bits"
        );
    }
    
    /// Get selected row count
    pub fn selected_count(&self) -> usize {
        self.row_count
    }
    
    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
    
    /// Slice batch to get a specific number of SELECTED rows
    /// This is selection-aware: it skips `offset` selected rows, then returns `length` selected rows.
    /// Unlike a simple array slice, this respects the selection bitmap.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        // Find which array indices correspond to the offset and offset+length selected rows
        let mut selected_count = 0;
        let mut start_array_idx = 0;
        let mut end_array_idx = self.selection.len();
        
        // Find the array index where we've skipped `offset` selected rows
        for (i, selected) in self.selection.iter().enumerate() {
            if *selected {
                if selected_count == offset {
                    start_array_idx = i;
                    break;
                }
                selected_count += 1;
            }
        }
        
        // Find the array index where we've collected `offset + length` selected rows
        let target = offset + length;
        selected_count = 0;
        for (i, selected) in self.selection.iter().enumerate() {
            if *selected {
                selected_count += 1;
                if selected_count == target {
                    end_array_idx = i + 1;
                    break;
                }
            }
        }
        
        trace!(
            offset = offset,
            length = length,
            start_array_idx = start_array_idx,
            end_array_idx = end_array_idx,
            "ExecutionBatch::slice"
        );
        
        // Slice the underlying batch and selection
        let slice_length = end_array_idx - start_array_idx;
        let batch = self.batch.slice(start_array_idx, slice_length);
        let selection_slice = &self.selection[start_array_idx..end_array_idx];
        let mut new_selection = bitvec::prelude::BitVec::with_capacity(slice_length);
        new_selection.extend_from_bitslice(selection_slice);
        let row_count = new_selection.count_ones();
        
        trace!(
            new_selection_len = new_selection.len(),
            row_count = row_count,
            "ExecutionBatch::slice: result"
        );
        
        Self {
            batch,
            selection: new_selection,
            row_count,
            column_fragments: self.column_fragments.clone(), // Fragments are shared
            column_schema: self.column_schema.clone(), // COLID: Preserve ColumnSchema
        }
    }
    
    /// Get fragment for a column (for dictionary encoding lookup)
    pub fn get_column_fragment(&self, column_name: &str) -> Option<&Arc<ColumnFragment>> {
        self.column_fragments.get(column_name)
    }
    
    /// COLID: Create ExecutionBatch from ColumnarBatch with optional ColumnSchema
    pub fn from_batch_with_column_schema(
        batch: ColumnarBatch, 
        column_schema: Option<ColumnSchema>
    ) -> Self {
        let row_count = batch.row_count;
        let selection = bitvec![1; row_count];
        
        Self {
            batch,
            selection,
            row_count,
            column_fragments: HashMap::new(),
            column_schema,
        }
    }
}

/// Batch iterator - produces batches of rows
/// BatchIterator trait - core contract for all operators
/// 
/// # Termination Contract (DuckDB/Presto-style)
/// 
/// All implementations MUST guarantee:
/// 1. next() must eventually return Ok(None) after a finite number of Ok(Some(batch)) calls
/// 2. next() must not return Ok(Some(batch)) with row_count == 0 in an infinite loop
/// 3. If an operator cannot produce more batches, it MUST return Ok(None)
/// 
/// Violations of this contract indicate bugs and will be detected by termination guards.
pub trait BatchIterator: Send {
    /// Get next batch
    fn next(&mut self) -> Result<Option<ExecutionBatch>, anyhow::Error>;
    
    /// Get schema
    fn schema(&self) -> SchemaRef;
    
    /// Prepare the operator (ExecNode lifecycle)
    /// Each operator must implement this to call prepare() on children
    fn prepare(&mut self) -> Result<(), anyhow::Error>;
}

