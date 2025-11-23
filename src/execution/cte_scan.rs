/// CTE Scan Operator - reads from cached CTE results
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use anyhow::Result;
use arrow::datatypes::*;
use std::sync::Arc;

/// CTE Scan operator - scans cached CTE results stored as ExecutionBatch vectors
pub struct CTEScanOperator {
    /// Cached CTE batches (owned by the operator)
    batches: Vec<ExecutionBatch>,
    /// Columns to project from CTE
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

impl CTEScanOperator {
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

impl BatchIterator for CTEScanOperator {
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
                
                // If we've consumed the batch, move to next
                if self.current_row_idx >= batch.row_count {
                    self.current_batch_idx += 1;
                    self.current_row_idx = 0;
                }
                
                // Apply LIMIT by truncating if needed
                if let Some(limit) = self.limit {
                    if self.rows_returned > limit {
                        // Return partial batch
                        // TODO: Actually slice the batch
                        // For now, we'll let the LIMIT operator handle it
                    }
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
        // SCHEMA-FLOW: CTEScanOperator returns schema from materialized batches
        // Return schema from first batch (all batches should have same schema)
        if let Some(first_batch) = self.batches.first() {
            let schema = first_batch.batch.schema.clone();
            
            // SCHEMA-FLOW DEBUG: Log schema from CTE
            eprintln!("DEBUG CTEScanOperator::schema() - CTE schema has {} fields: {:?}", 
                schema.fields().len(),
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            
            schema
        } else {
            // Empty CTE - create schema from column names
            let fields: Vec<Field> = self.columns.iter()
                .map(|col| Field::new(col, DataType::Utf8, true))
                .collect();
            Arc::new(Schema::new(fields))
        }
    }
}

