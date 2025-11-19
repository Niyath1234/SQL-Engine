/// HAVING Clause Support
/// Filters groups after aggregation
use crate::execution::batch::{ExecutionBatch, BatchIterator};
use crate::query::expression::{Expression, ExpressionEvaluator};
use crate::storage::fragment::Value;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;
use bitvec::prelude::*;

/// HAVING operator - filters groups after aggregation
pub struct HavingOperator {
    input: Box<dyn BatchIterator>,
    predicate: Expression,
    evaluator: ExpressionEvaluator,
}

impl HavingOperator {
    pub fn new(
        input: Box<dyn BatchIterator>,
        predicate: Expression,
    ) -> Self {
        let schema = input.schema();
        let evaluator = ExpressionEvaluator::new(schema);
        Self {
            input,
            predicate,
            evaluator,
        }
    }
}

impl BatchIterator for HavingOperator {
    fn next(&mut self) -> Result<Option<ExecutionBatch>> {
        let batch = match self.input.next()? {
            Some(b) => b,
            None => return Ok(None),
        };
        
        // Evaluate HAVING predicate for each row (group)
        let mut selection = BitVec::new();
        selection.reserve(batch.row_count);
        
        for row_idx in 0..batch.row_count {
            if batch.selection[row_idx] {
                // Evaluate predicate
                match self.evaluator.evaluate(&self.predicate, &batch, row_idx) {
                    Ok(Value::Int64(1)) | Ok(Value::Int64(-1)) => {
                        // True
                        selection.push(true);
                    }
                    Ok(Value::Int64(0)) | Ok(Value::Null) => {
                        // False or NULL
                        selection.push(false);
                    }
                    _ => {
                        // Non-boolean result - treat as false
                        selection.push(false);
                    }
                }
            } else {
                selection.push(false);
            }
        }
        
        // Apply selection
        let mut filtered_batch = batch;
        filtered_batch.apply_selection(&selection);
        
        if filtered_batch.row_count == 0 {
            Ok(None)
        } else {
            Ok(Some(filtered_batch))
        }
    }
    
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

