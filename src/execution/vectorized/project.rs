/// VectorProjectOperator: Async, vectorized projection operator
/// 
/// This is the Phase 1 replacement for ProjectOperator, designed for:
/// - Zero-copy column selection
/// - Vectorized expression evaluation
/// - Wildcard expansion
/// - Column renaming
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use crate::query::plan::ProjectionExpr;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use tracing::{debug, info};

/// Vector project operator - async, vectorized projection
pub struct VectorProjectOperator {
    /// Input operator
    input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    
    /// Projection expressions
    expressions: Vec<ProjectionExpr>,
    
    /// Column names (for wildcard expansion)
    columns: Vec<String>,
    
    /// Prepared flag
    prepared: bool,
}

impl VectorProjectOperator {
    /// Create a new vector project operator
    pub fn new(
        input: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
        columns: Vec<String>,
        expressions: Vec<ProjectionExpr>,
    ) -> Self {
        Self {
            input,
            columns,
            expressions,
            prepared: false,
        }
    }
    
    /// Project a batch (vectorized, zero-copy where possible)
    async fn project_batch(&self, batch: &VectorBatch) -> EngineResult<VectorBatch> {
        let mut output_arrays = Vec::new();
        let mut output_fields: Vec<Field> = Vec::new();
        
        // Handle each projection expression
        for expr in &self.expressions {
            match &expr.expr_type {
                crate::query::plan::ProjectionExprType::Column(col_name) => {
                    if col_name == "*" {
                        // Wildcard - include all columns
                        for (idx, field) in batch.schema.fields().iter().enumerate() {
                            let array = batch.column(idx)?;
                            output_arrays.push(array);
                            output_fields.push(field.as_ref().clone());
                        }
                    } else {
                        // Single column - find it in input
                        let col_idx = batch.schema.fields()
                            .iter()
                            .position(|f| f.name() == col_name.as_str())
                            .ok_or_else(|| crate::error::EngineError::execution(format!(
                                "Column '{}' not found in batch schema",
                                col_name
                            )))?;
                        
                        let array = batch.column(col_idx)?;
                        let data_type = array.data_type().clone();
                        output_arrays.push(array);
                        
                        // Use alias if provided, otherwise use column name
                        let field_name: &str = if !expr.alias.is_empty() {
                            expr.alias.as_str()
                        } else {
                            col_name.as_str()
                        };
                        output_fields.push(Field::new(
                            field_name,
                            data_type,
                            true,
                        ));
                    }
                }
                crate::query::plan::ProjectionExprType::Cast { column, .. } => {
                    // CAST expression - find column and cast it
                    // For now, just use the column (casting will be implemented later)
                    let col_idx = batch.schema.fields()
                        .iter()
                        .position(|f| f.name() == column.as_str())
                        .ok_or_else(|| crate::error::EngineError::execution(format!(
                            "Column '{}' not found in batch schema for CAST",
                            column
                        )))?;
                    
                    let array = batch.column(col_idx)?;
                    let data_type = array.data_type().clone();
                    output_arrays.push(array);
                    
                    // Use alias if provided, otherwise use column name
                    let field_name: &str = if !expr.alias.is_empty() {
                        expr.alias.as_str()
                    } else {
                        column.as_str()
                    };
                    output_fields.push(Field::new(
                        field_name,
                        data_type,
                        true,
                    ));
                }
                _ => {
                    // Other expression types - for now, return error
                    // TODO: Implement expression evaluation
                    return Err(crate::error::EngineError::execution(
                        "Expression evaluation not yet implemented in VectorProjectOperator"
                    ));
                }
            }
        }
        
        // Create output schema
        let output_schema = Arc::new(Schema::new(output_fields));
        
        // Create output batch (zero-copy: arrays are Arc)
        let output_batch = VectorBatch::new(output_arrays, output_schema);
        
        debug!(
            input_columns = batch.schema.fields().len(),
            output_columns = output_batch.schema.fields().len(),
            "Projected batch"
        );
        
        Ok(output_batch)
    }
}

#[async_trait::async_trait]
impl VectorOperator for VectorProjectOperator {
    fn schema(&self) -> SchemaRef {
        // Build output schema from expressions
        // For now, use placeholder - will be set during prepare()
        let fields: Vec<Field> = self.expressions.iter()
            .map(|expr| {
                let name: &str = if !expr.alias.is_empty() {
                    expr.alias.as_str()
                } else {
                    match &expr.expr_type {
                        crate::query::plan::ProjectionExprType::Column(col) => col.as_str(),
                        crate::query::plan::ProjectionExprType::Cast { column, .. } => column.as_str(),
                        _ => "expr",
                    }
                };
                Field::new(name, DataType::Utf8, true) // Placeholder type
            })
            .collect();
        Arc::new(Schema::new(fields))
    }
    
    async fn prepare(&mut self) -> EngineResult<()> {
        if self.prepared {
            return Ok(());
        }
        
        info!(
            expression_count = self.expressions.len(),
            "Preparing vector project operator"
        );
        
        // Recursively prepare input
        {
            let mut input = self.input.lock().await;
            input.prepare().await?;
        }
        
        self.prepared = true;
        debug!("Vector project operator prepared");
        Ok(())
    }
    
    async fn next_batch(&mut self) -> EngineResult<Option<VectorBatch>> {
        if !self.prepared {
            return Err(crate::error::EngineError::execution(
                "VectorProjectOperator::next_batch() called before prepare()"
            ));
        }
        
        // Get next batch from input
        let input_batch = {
            let mut input = self.input.lock().await;
            input.next_batch().await?
        };
        
        let Some(batch) = input_batch else {
            return Ok(None); // Input exhausted
        };
        
        // Project batch
        let projected_batch = self.project_batch(&batch).await?;
        
        Ok(Some(projected_batch))
    }
    
    fn children_mut(&mut self) -> Vec<&mut dyn VectorOperator> {
        // Cannot return mutable reference through Arc<Mutex>
        vec![]
    }
}

