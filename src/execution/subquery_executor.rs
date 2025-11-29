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
    
    /// Rewrite subquery AST to replace outer-context column references with literal values
    /// This handles correlated subqueries by binding outer query values
    fn rewrite_subquery_with_outer_context(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        outer_batch: &ExecutionBatch,
    ) -> Result<Box<sqlparser::ast::Query>> {
        use sqlparser::ast::*;
        
        // Extract table aliases from subquery to identify which columns are from outer context
        let subquery_tables = self.extract_subquery_tables(subquery);
        eprintln!("DEBUG rewrite_subquery: subquery_tables={:?}, outer_batch schema fields: {:?}", 
            subquery_tables, 
            outer_batch.batch.schema.fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
        
        // Clone the subquery and rewrite it
        let mut rewritten = subquery.clone();
        
        // Rewrite WHERE clause to replace outer-context column references
        if let SetExpr::Select(select) = rewritten.body.as_mut() {
            if let Some(where_clause) = select.selection.as_mut() {
                let original_where = format!("{:?}", where_clause);
                *where_clause = self.rewrite_expr_with_outer_context(
                    where_clause,
                    outer_batch,
                    &subquery_tables,
                )?;
                let rewritten_where = format!("{:?}", where_clause);
                eprintln!("DEBUG rewrite_subquery: WHERE clause rewritten: {} -> {}", original_where, rewritten_where);
            } else {
                eprintln!("DEBUG rewrite_subquery: No WHERE clause in subquery");
            }
        }
        
        Ok(rewritten)
    }
    
    /// Extract table names/aliases from subquery to identify outer-context columns
    fn extract_subquery_tables(&self, query: &Box<sqlparser::ast::Query>) -> std::collections::HashSet<String> {
        use sqlparser::ast::*;
        let mut tables = std::collections::HashSet::new();
        
        if let SetExpr::Select(select) = query.body.as_ref() {
            // Extract from FROM clause
            for table_with_joins in &select.from {
                match &table_with_joins.relation {
                    TableFactor::Table { name, alias, .. } => {
                        // Add table name
                        let table_name = name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                        tables.insert(table_name);
                        // Add alias if present
                        if let Some(alias) = alias {
                            tables.insert(alias.name.value.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        
        tables
    }
    
    /// Rewrite expression to replace outer-context column references with literal values
    fn rewrite_expr_with_outer_context(
        &self,
        expr: &sqlparser::ast::Expr,
        outer_batch: &ExecutionBatch,
        subquery_tables: &std::collections::HashSet<String>,
    ) -> Result<sqlparser::ast::Expr> {
        use sqlparser::ast::*;
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let rewritten_left = Box::new(self.rewrite_expr_with_outer_context(left, outer_batch, subquery_tables)?);
                let rewritten_right = Box::new(self.rewrite_expr_with_outer_context(right, outer_batch, subquery_tables)?);
                Ok(Expr::BinaryOp {
                    left: rewritten_left,
                    op: op.clone(),
                    right: rewritten_right,
                })
            }
            Expr::CompoundIdentifier(idents) => {
                // Check if this column reference is from outer context
                if idents.len() >= 2 {
                    let table_alias = &idents[0].value;
                    let column_name = &idents[1].value;
                    
                    eprintln!("DEBUG rewrite_expr: Checking CompoundIdentifier: {}.{} (subquery_tables: {:?})", 
                        table_alias, column_name, subquery_tables);
                    
                    // If table alias is not in subquery tables, it's from outer context
                    if !subquery_tables.contains(table_alias) {
                        eprintln!("DEBUG rewrite_expr: {}.{} is from outer context, resolving from outer batch", 
                            table_alias, column_name);
                        
                        // Resolve column from outer batch
                        let column_value = self.resolve_column_from_outer_context(
                            outer_batch,
                            table_alias,
                            column_name,
                        )?;
                        
                        eprintln!("DEBUG rewrite_expr: Resolved {}.{} = {:?}", table_alias, column_name, column_value);
                        
                        // Convert Value to sqlparser::ast::Value
                        use sqlparser::ast::Value as SqlValue;
                        use crate::storage::fragment::Value as FragmentValue;
                        let literal_value = match column_value {
                            FragmentValue::Int64(i) => SqlValue::Number(i.to_string(), false),
                            FragmentValue::Int32(i) => SqlValue::Number(i.to_string(), false),
                            FragmentValue::Float64(f) => {
                                // For integers stored as Float64, format without decimal
                                if f == f.floor() && f.abs() < 1e15 {
                                    SqlValue::Number(format!("{:.0}", f), false)
                                } else {
                                    SqlValue::Number(format!("{:.15}", f), false)
                                }
                            }
                            FragmentValue::Float32(f) => {
                                if f == f.floor() && f.abs() < 1e15 {
                                    SqlValue::Number(format!("{:.0}", f as f64), false)
                                } else {
                                    SqlValue::Number(format!("{:.15}", f as f64), false)
                                }
                            }
                            FragmentValue::String(s) => SqlValue::SingleQuotedString(s),
                            FragmentValue::Bool(b) => SqlValue::Boolean(b),
                            FragmentValue::Null => SqlValue::Null,
                            FragmentValue::Vector(_) => {
                                return Err(anyhow::anyhow!("Vector values cannot be used in subquery WHERE clauses"));
                            }
                        };
                        
                        eprintln!("DEBUG rewrite_expr: Converted to SqlValue: {:?}", literal_value);
                        
                        return Ok(Expr::Value(literal_value));
                    } else {
                        eprintln!("DEBUG rewrite_expr: {}.{} is from subquery tables, keeping as is", 
                            table_alias, column_name);
                    }
                }
                Ok(expr.clone())
            }
            Expr::Identifier(_) => {
                // Unqualified identifier - could be from outer context, but we can't tell
                // For now, leave it as is (will be resolved during execution)
                Ok(expr.clone())
            }
            _ => {
                // For other expression types, recursively rewrite
                // This is a simplified version - in production, we'd need to handle all expression types
                Ok(expr.clone())
            }
        }
    }
    
    /// Resolve a column value from outer context batch
    fn resolve_column_from_outer_context(
        &self,
        outer_batch: &ExecutionBatch,
        table_alias: &str,
        column_name: &str,
    ) -> Result<Value> {
        use arrow::array::*;
        use arrow::datatypes::*;
        
        // Try to find column in outer batch schema
        // First try qualified name: table_alias.column_name
        let qualified_name = format!("{}.{}", table_alias, column_name);
        let col_idx = outer_batch.batch.schema.index_of(&qualified_name)
            .or_else(|_| {
                // Try unqualified column name
                outer_batch.batch.schema.index_of(column_name)
            })
            .or_else(|_| {
                // Try to find by suffix (for qualified schemas)
                for (idx, field) in outer_batch.batch.schema.fields().iter().enumerate() {
                    if field.name().ends_with(&format!(".{}", column_name)) {
                        return Ok(idx);
                    }
                }
                Err(arrow::error::ArrowError::SchemaError(format!("Column not found")))
            })?;
        
        // Extract value from first row (outer context is single-row batch)
        let col = outer_batch.batch.column(col_idx)
            .ok_or_else(|| anyhow::anyhow!("Column index {} out of range", col_idx))?;
        
        // Get value from row 0 (single-row batch)
        if col.is_null(0) {
            return Ok(Value::Null);
        }
        
        match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
                Ok(Value::Int64(arr.value(0)))
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
                Ok(Value::Float64(arr.value(0)))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
                Ok(Value::String(arr.value(0).to_string()))
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast to BooleanArray"))?;
                Ok(Value::Bool(arr.value(0)))
            }
            _ => anyhow::bail!("Unsupported data type for outer context column: {:?}", col.data_type())
        }
    }
}

impl SubqueryExecutor for DefaultSubqueryExecutor {
    fn execute_scalar_subquery(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        outer_context: Option<&ExecutionBatch>,
    ) -> Result<Option<Value>> {
        // If outer_context is provided, rewrite subquery to replace outer-context column references
        let rewritten_subquery = if let Some(outer_batch) = outer_context {
            self.rewrite_subquery_with_outer_context(subquery, outer_batch)?
        } else {
            subquery.clone()
        };
        
        // Convert Query (Box<Query>) to Statement for extraction
        let ast = sqlparser::ast::Statement::Query(rewritten_subquery);
        let parsed = extract_query_info_enhanced(&ast)?;
        
        // Plan the subquery
        // Create a mutable planner for planning (planner.plan_with_ast requires &mut)
        // Note: Subqueries don't have CTE context - they're executed independently
        let mut planner_for_planning = QueryPlanner::from_arc_with_options(self.graph.clone(), false);
        let subquery_plan = planner_for_planning.plan_with_ast(&parsed, Some(&ast))?;
        
        // Execute the subquery (create engine instance)
        // Pass self as subquery executor for nested subqueries
        let engine = self.create_engine();
        let executor = Arc::new(DefaultSubqueryExecutor::with_graph(
            self.graph.clone(),
            // Create new planner (planner is not Clone)
            QueryPlanner::from_arc_with_options(self.graph.clone(), false),
        ));
        eprintln!("DEBUG subquery executor: Executing subquery with outer_context.is_some()={}", outer_context.is_some());
        let result = engine.execute_with_subquery_executor(
            &subquery_plan,
            None, // max_time_ms
            None, // max_scan_rows
            None, // cte_results (subqueries don't have CTEs)
            Some(executor.clone() as Arc<dyn crate::query::expression::SubqueryExecutor>),
        )?;
        
        eprintln!("DEBUG subquery executor: Subquery executed, row_count={}, batches.len()={}", result.row_count, result.batches.len());
        
        // Extract first row, first column value
        if result.row_count == 0 {
            eprintln!("DEBUG subquery executor: Subquery returned 0 rows");
            return Ok(None);  // Subquery returned no rows
        }
        
        // Get first batch, first row, first column
        if let Some(first_batch) = result.batches.first() {
            eprintln!("DEBUG subquery executor: First batch has {} rows, {} columns", first_batch.row_count, first_batch.batch.columns.len());
            eprintln!("DEBUG subquery executor: Schema fields: {:?}", first_batch.batch.schema.fields().iter().map(|f| f.name().to_string()).collect::<Vec<_>>());
            if first_batch.row_count > 0 {
                // Find first selected row (or first row if no selection bitmap)
                let mut found_row = false;
                for row_idx in 0..first_batch.row_count {
                    // Check selection bitmap if present, otherwise use first row
                    let is_selected = if row_idx < first_batch.selection.len() {
                        first_batch.selection[row_idx]
                    } else {
                        true // If no selection bitmap, assume all rows are selected
                    };
                    
                    if is_selected {
                        found_row = true;
                        eprintln!("DEBUG subquery executor: Processing row {} (selected={}, row_count={})", row_idx, is_selected, first_batch.row_count);
                        // Try each column until we find a non-null value (aggregate results might not be in column 0)
                        for col_idx in 0..first_batch.batch.columns.len() {
                            if let Some(col) = first_batch.batch.column(col_idx) {
                                let column_name = first_batch.batch.schema.field(col_idx).name();
                                // Skip "window_result" or other non-aggregate columns
                                if column_name == "window_result" || column_name.starts_with("_") {
                                    eprintln!("DEBUG subquery executor: Skipping column {} ({})", col_idx, column_name);
                                    continue;
                                }
                                // Check if the array actually has data
                                eprintln!("DEBUG subquery executor: Column {} ({}): array.len()={}, row_idx={}, is_null={}", 
                                    col_idx, column_name, col.len(), row_idx, col.is_null(row_idx));
                                
                                // If array has valid data, try to extract value
                                if row_idx < col.len() && !col.is_null(row_idx) {
                                    let value = extract_value_from_array(col, row_idx)?;
                                    eprintln!("DEBUG subquery executor: Column {} ({}): extracted value={:?}", col_idx, column_name, value);
                                    if !matches!(value, Value::Null) {
                                        eprintln!("DEBUG subquery executor: Using non-null value from column {}: {:?}", col_idx, value);
                                        return Ok(Some(value));
                                    }
                                } else {
                                    eprintln!("DEBUG subquery executor: Column {} ({}) is NULL or out of bounds (row_idx={}, array.len()={})", 
                                        col_idx, column_name, row_idx, col.len());
                                }
                            }
                        }
                        // If all columns were NULL but we have a row, return NULL
                        eprintln!("DEBUG subquery executor: All columns are NULL for row {}", row_idx);
                        return Ok(Some(Value::Null));
                    }
                }
                if !found_row {
                    eprintln!("DEBUG subquery executor: No selected rows found in batch (row_count={}, selection.len()={})", first_batch.row_count, first_batch.selection.len());
                }
            }
        }
        
        eprintln!("DEBUG subquery executor: Returning None (fallback)");
        Ok(None)
    }
    
    fn execute_exists_subquery(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        outer_context: Option<&ExecutionBatch>,
    ) -> Result<bool> {
        // If outer_context is provided, rewrite subquery to replace outer-context column references
        let rewritten_subquery = if let Some(outer_batch) = outer_context {
            self.rewrite_subquery_with_outer_context(subquery, outer_batch)?
        } else {
            subquery.clone()
        };
        
        // Convert Query (Box<Query>) to Statement for extraction
        let ast = sqlparser::ast::Statement::Query(rewritten_subquery);
        let parsed = extract_query_info_enhanced(&ast)?;
        
        // Plan the subquery
        // Create a mutable planner for planning (planner.plan_with_ast requires &mut)
        // Note: Subqueries don't have CTE context - they're executed independently
        let mut planner_for_planning = QueryPlanner::from_arc_with_options(self.graph.clone(), false);
        let subquery_plan = planner_for_planning.plan_with_ast(&parsed, Some(&ast))?;
        
        // Execute the subquery with LIMIT 1 for optimization (create engine instance)
        // Pass self as subquery executor for nested subqueries
        let engine = self.create_engine();
        let executor = Arc::new(DefaultSubqueryExecutor::with_graph(
            self.graph.clone(),
            // Create new planner (planner is not Clone)
            QueryPlanner::from_arc_with_options(self.graph.clone(), false),
        ));
        let result = engine.execute_with_subquery_executor(
            &subquery_plan,
            None, // max_time_ms
            None, // max_scan_rows
            None, // cte_results (subqueries don't have CTEs)
            Some(executor.clone() as Arc<dyn crate::query::expression::SubqueryExecutor>),
        )?;
        
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

