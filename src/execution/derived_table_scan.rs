/// Derived Table Scan Operator - reads from executed subquery results
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use anyhow::Result;
use arrow::datatypes::*;
use std::sync::Arc;

/// Derived Table Scan operator - scans cached derived table results stored as ExecutionBatch vectors
pub struct DerivedTableScanOperator {
    /// Cached derived table batches (owned by the operator)
    batches: Vec<ExecutionBatch>,
    /// Columns to project from derived table
    columns: Vec<String>,
    /// Current batch index
    current_batch_idx: usize,
    /// Current row index within current batch
    current_row_idx: usize,
    /// LIMIT (if specified)
    limit: Option<usize>,
    /// OFFSET (if specified)
    offset: Option<usize>,
    /// Rows returned so far (for LIMIT/OFFSET tracking)
    rows_returned: usize,
    /// Rows skipped so far (for OFFSET)
    rows_skipped: usize,
}

impl DerivedTableScanOperator {
    pub fn new(
        batches: Vec<ExecutionBatch>,
        columns: Vec<String>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        Self {
            batches,
            columns,
            current_batch_idx: 0,
            current_row_idx: 0,
            limit,
            offset,
            rows_returned: 0,
            rows_skipped: 0,
        }
    }
}

impl BatchIterator for DerivedTableScanOperator {
    fn prepare(&mut self) -> Result<(), anyhow::Error> {
        Ok(()) // DerivedTableScanOperator doesn't need prepare() yet
    }
    
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        // If LIMIT is reached, return None
        if let Some(limit) = self.limit {
            if self.rows_returned >= limit {
                return Ok(None);
            }
        }
        
        // Skip batches until we've skipped enough rows for OFFSET
        while self.current_batch_idx < self.batches.len() {
            let batch = &self.batches[self.current_batch_idx];
            
            // Skip rows in current batch for OFFSET
            while self.rows_skipped < self.offset.unwrap_or(0) && self.current_row_idx < batch.row_count {
                self.current_row_idx += 1;
                self.rows_skipped += 1;
            }
            
            // If we've skipped all rows in this batch, move to next batch
            if self.current_row_idx >= batch.row_count {
                self.current_batch_idx += 1;
                self.current_row_idx = 0;
                continue;
            }
            
            // Check if we can return remaining rows from this batch
            let remaining_in_batch = batch.row_count - self.current_row_idx;
            let remaining_needed = if let Some(limit) = self.limit {
                limit - self.rows_returned
            } else {
                remaining_in_batch
            };
            
            let rows_to_return = remaining_in_batch.min(remaining_needed);
            
            if rows_to_return > 0 {
                // Create a slice of the batch with only the rows we need
                // For now, we'll return the full batch and let projection handle column selection
                // TODO: Optimize to slice rows and columns
                
                let result_batch = if self.current_row_idx == 0 && rows_to_return == batch.row_count {
                    // Return the whole batch
                    batch.clone()
                } else {
                    // TODO: Create a slice of the batch
                    // For now, return the whole batch and rely on LIMIT operator
                    batch.clone()
                };
                
                // Update tracking
                self.rows_returned += result_batch.row_count;
                self.current_row_idx += result_batch.row_count;
                
                // Move to next batch if we've exhausted this one
                if self.current_row_idx >= batch.row_count {
                    self.current_batch_idx += 1;
                    self.current_row_idx = 0;
                }
                
                return Ok(Some(result_batch));
            }
            
            // Move to next batch
            self.current_batch_idx += 1;
            self.current_row_idx = 0;
        }
        
        // No more batches
        Ok(None)
    }
    
    fn schema(&self) -> SchemaRef {
        // Return schema from first batch if available
        if let Some(first_batch) = self.batches.first() {
            first_batch.batch.schema.clone()
        } else {
            // Empty schema if no batches
            Arc::new(Schema::empty())
        }
    }
}

