/// Subquery Executor - executes scalar and EXISTS subqueries
use crate::query::expression::SubqueryExecutor;
use crate::storage::fragment::Value;
use crate::execution::batch::ExecutionBatch;
use crate::execution::engine::ExecutionEngine;
use crate::query::parser_enhanced::extract_query_info_enhanced;
use crate::query::planner::QueryPlanner;
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;
use std::sync::Arc;

/// Default implementation of SubqueryExecutor using ExecutionEngine
pub struct DefaultSubqueryExecutor {
    graph: Arc<HyperGraph>,
    planner: QueryPlanner,
}

impl DefaultSubqueryExecutor {
    /// Create with ExecutionEngine (will extract graph if possible)
    /// Note: This is a compatibility method - prefer using with_graph instead
    pub fn new(_engine: Arc<ExecutionEngine>, planner: QueryPlanner) -> Self {
        // Cannot access engine.graph directly (it's private)
        // This method is kept for compatibility but should not be used
        // Use with_graph() instead
        unreachable!("Use DefaultSubqueryExecutor::with_graph() - graph is private in ExecutionEngine")
    }
    
    /// Create with HyperGraph directly (preferred method)
    pub fn with_graph(graph: Arc<HyperGraph>, planner: QueryPlanner) -> Self {
        Self {
            graph,
            planner,
        }
    }
    
    fn create_engine(&self) -> ExecutionEngine {
        ExecutionEngine::from_arc(self.graph.clone())
    }
}

impl SubqueryExecutor for DefaultSubqueryExecutor {
    fn execute_scalar_subquery(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        _outer_context: Option<&ExecutionBatch>,  // TODO: Handle correlated subqueries
    ) -> Result<Option<Value>> {
        // Convert Query (Box<Query>) to Statement for extraction
        let ast = sqlparser::ast::Statement::Query(subquery.clone());
        let parsed = extract_query_info_enhanced(&ast)?;
        
        // Plan the subquery
        let subquery_plan = self.planner.plan_with_ast(&parsed, Some(&ast))?;
        
        // Execute the subquery (create engine instance)
        let engine = self.create_engine();
        let result = engine.execute(&subquery_plan)?;
        
        // Extract first row, first column value
        if result.row_count == 0 {
            return Ok(None);  // Subquery returned no rows
        }
        
        // Get first batch, first row, first column
        if let Some(first_batch) = result.batches.first() {
            if first_batch.row_count > 0 {
                // Find first selected row
                for row_idx in 0..first_batch.row_count {
                    if row_idx < first_batch.selection.len() && first_batch.selection[row_idx] {
                        // Get first column value
                        if let Some(first_column) = first_batch.batch.column(0) {
                            return Ok(Some(extract_value_from_array(first_column, row_idx)?));
                        }
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    fn execute_exists_subquery(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        _outer_context: Option<&ExecutionBatch>,  // TODO: Handle correlated subqueries
    ) -> Result<bool> {
        // Convert Query (Box<Query>) to Statement for extraction
        let ast = sqlparser::ast::Statement::Query(subquery.clone());
        let parsed = extract_query_info_enhanced(&ast)?;
        
        // Plan the subquery
        let subquery_plan = self.planner.plan_with_ast(&parsed, Some(&ast))?;
        
        // Execute the subquery with LIMIT 1 for optimization (create engine instance)
        let engine = self.create_engine();
        let result = engine.execute(&subquery_plan)?;
        
        // EXISTS returns true if any rows were returned
        Ok(result.row_count > 0)
    }
}

/// Helper function to extract Value from Arrow array
fn extract_value_from_array(array: &Arc<dyn arrow::array::Array>, row_idx: usize) -> Result<Value> {
    use arrow::array::*;
    use arrow::datatypes::*;
    
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }
    
    // Handle different array types based on data type
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            Ok(Value::Int64(arr.value(row_idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            Ok(Value::Float64(arr.value(row_idx)))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
            Ok(Value::String(arr.value(row_idx).to_string()))
        }
        _ => anyhow::bail!("Unsupported array type for scalar subquery extraction: {:?}", array.data_type())
    }
}

