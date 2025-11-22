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
        Self::with_subquery_executor(input, predicate, None)
    }
    
    pub fn with_subquery_executor(
        input: Box<dyn BatchIterator>,
        predicate: Expression,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
    ) -> Self {
        Self::with_subquery_executor_and_aliases(input, predicate, subquery_executor, std::collections::HashMap::new())
    }
    
    pub fn with_subquery_executor_and_aliases(
        input: Box<dyn BatchIterator>,
        predicate: Expression,
        subquery_executor: Option<std::sync::Arc<dyn crate::query::expression::SubqueryExecutor>>,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        let schema = input.schema();
        let evaluator = if let Some(executor) = subquery_executor {
            ExpressionEvaluator::with_subquery_executor_and_aliases(schema, executor, table_aliases)
        } else {
            ExpressionEvaluator::with_table_aliases(schema, table_aliases)
        };
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
                // Evaluate predicate (should already be rewritten to use column references)
                match self.evaluator.evaluate(&self.predicate, &batch, row_idx) {
                    Ok(Value::Int64(1)) | Ok(Value::Int64(-1)) => {
                        // True - keep this row
                        selection.push(true);
                    }
                    Ok(Value::Int64(0)) | Ok(Value::Null) => {
                        // False or NULL - filter out this row
                        selection.push(false);
                    }
                    Ok(Value::Bool(true)) => {
                        // Boolean true
                        selection.push(true);
                    }
                    Ok(Value::Bool(false)) => {
                        // Boolean false
                        selection.push(false);
                    }
                    Ok(other) => {
                        // Non-boolean result - check if it's truthy (non-zero number, non-empty string, etc.)
                        let is_true = match other {
                            Value::Int64(i) => i != 0,
                            Value::Float64(f) => f != 0.0,
                            Value::String(s) => !s.is_empty(),
                            Value::Bool(b) => b,
                            Value::Null => false,
                            _ => false,
                        };
                        selection.push(is_true);
                    }
                    Err(e) => {
                        // Evaluation error - filter out this row and log error
                        eprintln!("HAVING predicate evaluation error: {}", e);
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

