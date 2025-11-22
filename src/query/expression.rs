/// SQL Expression Evaluation
/// Handles evaluation of SQL expressions including functions, operators, and subqueries
use crate::storage::fragment::Value;
use crate::execution::batch::ExecutionBatch;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;

/// SQL Expression representation
#[derive(Clone, Debug)]
pub enum Expression {
    /// Column reference (table.column or column)
    Column(String, Option<String>),
    /// Literal value
    Literal(Value),
    /// Binary operation (left op right)
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation (op expr)
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    /// Function call
    Function {
        name: String,
        args: Vec<Expression>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expression>>,
        conditions: Vec<(Expression, Expression)>, // (condition, result)
        else_result: Option<Box<Expression>>,
    },
    /// Subquery (scalar)
    Subquery(Box<sqlparser::ast::Query>),
    /// EXISTS subquery
    Exists(Box<sqlparser::ast::Query>),
    /// IN subquery or IN list
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
        not: bool,
    },
    /// CAST expression
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
    },
    /// NULLIF expression
    NullIf {
        expr1: Box<Expression>,
        expr2: Box<Expression>,
    },
    /// COALESCE expression
    Coalesce(Vec<Expression>),
}

#[derive(Clone, Debug)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // String
    Concat,
    Like,
    ILike,
    Regexp,
    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    // Set
    In,
    NotIn,
}

#[derive(Clone, Debug)]
pub enum UnaryOperator {
    Not,
    Negate,
    Plus,
    BitwiseNot,
}

/// Subquery executor trait - allows ExpressionEvaluator to execute subqueries
pub trait SubqueryExecutor: Send + Sync {
    /// Execute a scalar subquery and return the first value
    /// Returns None if subquery returns no rows, Some(Value) if it returns one row/column
    fn execute_scalar_subquery(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        outer_context: Option<&ExecutionBatch>,  // For correlated subqueries
    ) -> Result<Option<Value>>;
    
    /// Execute an EXISTS subquery and return true/false
    fn execute_exists_subquery(
        &self,
        subquery: &Box<sqlparser::ast::Query>,
        outer_context: Option<&ExecutionBatch>,  // For correlated subqueries
    ) -> Result<bool>;
}

/// Expression evaluator
pub struct ExpressionEvaluator {
    /// Schema for resolving column references
    schema: SchemaRef,
    /// Optional subquery executor (for scalar/EXISTS subqueries)
    subquery_executor: Option<Arc<dyn SubqueryExecutor>>,
    /// Table alias mapping: alias -> actual table name (e.g., "d" -> "documents")
    table_aliases: std::collections::HashMap<String, String>,
}

impl ExpressionEvaluator {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            subquery_executor: None,
            table_aliases: std::collections::HashMap::new(),
        }
    }
    
    /// Create evaluator with subquery executor support
    pub fn with_subquery_executor(
        schema: SchemaRef,
        executor: Arc<dyn SubqueryExecutor>,
    ) -> Self {
        Self {
            schema,
            subquery_executor: Some(executor),
            table_aliases: std::collections::HashMap::new(),
        }
    }
    
    /// Create evaluator with table alias mapping
    pub fn with_table_aliases(
        schema: SchemaRef,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            schema,
            subquery_executor: None,
            table_aliases,
        }
    }
    
    /// Create evaluator with both subquery executor and table alias mapping
    pub fn with_subquery_executor_and_aliases(
        schema: SchemaRef,
        executor: Arc<dyn SubqueryExecutor>,
        table_aliases: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            schema,
            subquery_executor: Some(executor),
            table_aliases,
        }
    }
    
    /// Evaluate expression for a single row
    /// For subqueries, outer_context can be provided for correlated subquery support
    pub fn evaluate(
        &self,
        expr: &Expression,
        batch: &ExecutionBatch,
        row_idx: usize,
    ) -> Result<Value> {
        self.evaluate_with_context(expr, batch, row_idx, Some(batch))
    }
    
    /// Evaluate expression with optional outer context (for correlated subqueries)
    pub fn evaluate_with_context(
        &self,
        expr: &Expression,
        batch: &ExecutionBatch,
        row_idx: usize,
        outer_context: Option<&ExecutionBatch>,
    ) -> Result<Value> {
        match expr {
            Expression::Column(col_name, table_name) => {
                // Find column in schema
                let col_idx = if let Some(table) = table_name {
                    // Qualified column: table.column
                    // First, try to resolve table alias to actual table name
                    let actual_table = self.table_aliases.get(table).map(|s| s.as_str()).unwrap_or(table);
                    
                    // Try different formats:
                    // 1. alias.column (e.g., "d.id")
                    // 2. actual_table.column (e.g., "documents.id")
                    // 3. column (e.g., "id")
                    self.schema.index_of(&format!("{}.{}", table, col_name))
                        .or_else(|_| self.schema.index_of(&format!("{}.{}", actual_table, col_name)))
                        .or_else(|_| self.schema.index_of(col_name))
                } else {
                    // Unqualified column - try case-sensitive first, then case-insensitive
                    self.schema.index_of(col_name)
                        .or_else(|_| {
                            // Try case-insensitive match (for aggregate columns like COUNT vs count)
                            let upper_col = col_name.to_uppercase();
                            let lower_col = col_name.to_lowercase();
                            self.schema.index_of(&upper_col)
                                .or_else(|_| self.schema.index_of(&lower_col))
                                .or_else(|_| {
                                    // Try finding by case-insensitive partial match
                                    (0..self.schema.fields().len()).find(|&idx| {
                                        let field_name = self.schema.field(idx).name();
                                        field_name.to_uppercase() == upper_col || 
                                        field_name.to_lowercase() == lower_col
                                    }).ok_or_else(|| arrow::error::ArrowError::SchemaError(format!("Column not found: {}", col_name)))
                                })
                        })
                }
                .map_err(|e| anyhow::anyhow!("Column not found: {} ({})", col_name, e))?;
                
                // Get value from batch
                let array = batch.batch.column(col_idx)
                    .ok_or_else(|| anyhow::anyhow!("Column {} not found in batch", col_idx))?;
                
                extract_value_from_array(array, row_idx)
            }
            Expression::Literal(val) => Ok(val.clone()),
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_with_context(left, batch, row_idx, outer_context)?;
                let right_val = self.evaluate_with_context(right, batch, row_idx, outer_context)?;
                evaluate_binary_op(op, &left_val, &right_val)
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_with_context(expr, batch, row_idx, outer_context)?;
                evaluate_unary_op(op, &val)
            }
            Expression::Function { name, args } => {
                let arg_values: Result<Vec<Value>> = args.iter()
                    .map(|arg| self.evaluate_with_context(arg, batch, row_idx, outer_context))
                    .collect();
                evaluate_function(name, &arg_values?)
            }
            Expression::Case { operand, conditions, else_result } => {
                evaluate_case_with_context(operand.as_deref(), conditions, else_result.as_deref(), self, batch, row_idx, outer_context)
            }
            Expression::In { expr, list, not } => {
                let expr_val = self.evaluate_with_context(expr, batch, row_idx, outer_context)?;
                let mut found = false;
                for item in list {
                    if let Ok(item_val) = self.evaluate_with_context(item, batch, row_idx, outer_context) {
                        if expr_val == item_val {
                            found = true;
                            break;
                        }
                    }
                }
                Ok(Value::Int64(if *not { !found as i64 } else { found as i64 }))
            }
            Expression::Cast { expr, data_type } => {
                let val = self.evaluate_with_context(expr, batch, row_idx, outer_context)?;
                cast_value(&val, data_type)
            }
            Expression::NullIf { expr1, expr2 } => {
                let val1 = self.evaluate_with_context(expr1, batch, row_idx, outer_context)?;
                let val2 = self.evaluate_with_context(expr2, batch, row_idx, outer_context)?;
                if val1 == val2 {
                    Ok(Value::Null)
                } else {
                    Ok(val1)
                }
            }
            Expression::Coalesce(exprs) => {
                for expr in exprs {
                    let val = self.evaluate_with_context(expr, batch, row_idx, outer_context)?;
                    if !matches!(val, Value::Null) {
                        return Ok(val);
                    }
                }
                Ok(Value::Null)
            }
            Expression::Subquery(subquery) => {
                // Evaluate scalar subquery
                if let Some(ref executor) = self.subquery_executor {
                    // Create a single-row batch for correlated subquery context
                    let outer_batch = outer_context.map(|b| {
                        // Extract single row from outer context
                        // For now, use the full batch (will be filtered in subquery)
                        b.clone()
                    });
                    
                    match executor.execute_scalar_subquery(&subquery, outer_batch.as_ref()) {
                        Ok(Some(val)) => Ok(val),
                        Ok(None) => Ok(Value::Null),  // Subquery returned no rows
                        Err(e) => Err(e),
                    }
                } else {
                    anyhow::bail!("Subquery executor not provided - cannot execute scalar subquery")
                }
            }
            Expression::Exists(subquery) => {
                // EXISTS subquery - check if subquery returns any rows
                if let Some(ref executor) = self.subquery_executor {
                    let outer_batch = outer_context.map(|b| b.clone());
                    match executor.execute_exists_subquery(&subquery, outer_batch.as_ref()) {
                        Ok(exists) => Ok(Value::Int64(exists as i64)),
                        Err(e) => Err(e),
                    }
                } else {
                    anyhow::bail!("Subquery executor not provided - cannot execute EXISTS subquery")
                }
            }
        }
    }
    
    /// Evaluate expression for all rows in batch (vectorized)
    pub fn evaluate_vectorized(&self, expr: &Expression, batch: &ExecutionBatch) -> Result<Arc<dyn Array>> {
        // For now, fall back to row-by-row evaluation
        // TODO: Implement vectorized evaluation
        let mut values = Vec::new();
        for row_idx in 0..batch.row_count {
            if batch.selection[row_idx] {
                values.push(Some(self.evaluate(expr, batch, row_idx)?));
            } else {
                values.push(None);
            }
        }
        
        // Convert to Arrow array based on value types
        if values.is_empty() {
            return Ok(Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)));
        }
        
        // Determine type from first non-null value
        let first_val = values.iter().find_map(|v| v.as_ref());
        match first_val {
            Some(Value::Int64(_)) => {
                let arr: Vec<Option<i64>> = values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::Int64(i) => Some(*i),
                        _ => None,
                    })
                }).collect();
                Ok(Arc::new(Int64Array::from(arr)))
            }
            Some(Value::Float64(_)) => {
                let arr: Vec<Option<f64>> = values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::Float64(f) => Some(*f),
                        _ => None,
                    })
                }).collect();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            Some(Value::String(_)) => {
                let arr: Vec<Option<String>> = values.iter().map(|v| {
                    v.as_ref().and_then(|val| match val {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                }).collect();
                Ok(Arc::new(StringArray::from(arr)))
            }
            _ => Ok(Arc::new(Int64Array::from(vec![None; values.len()])))
        }
    }
}

fn extract_value_from_array(array: &Arc<dyn Array>, idx: usize) -> Result<Value> {
    if array.is_null(idx) {
        return Ok(Value::Null);
    }
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            Ok(Value::Int64(arr.value(idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            Ok(Value::Float64(arr.value(idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
            Ok(Value::String(arr.value(idx).to_string()))
        }
        DataType::FixedSizeList(field, size) => {
            // Extract vector from FixedSizeListArray
            let list_arr = array.as_any().downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to FixedSizeListArray"))?;
            
            let value_array = list_arr.values();
            let dimension = *size as usize;
            let start_idx = idx * dimension;
            let end_idx = start_idx + dimension;
            
            // Check if the value array contains Float32 or Float64
            match field.data_type() {
                DataType::Float32 => {
                    let float_arr = value_array.as_any().downcast_ref::<Float32Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast value array to Float32Array"))?;
                    let mut vec = Vec::with_capacity(dimension);
                    for i in start_idx..end_idx {
                        vec.push(float_arr.value(i));
                    }
                    Ok(Value::Vector(vec))
                }
                DataType::Float64 => {
                    let float_arr = value_array.as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to downcast value array to Float64Array"))?;
                    let mut vec = Vec::with_capacity(dimension);
                    for i in start_idx..end_idx {
                        vec.push(float_arr.value(i) as f32);
                    }
                    Ok(Value::Vector(vec))
                }
                _ => anyhow::bail!("FixedSizeListArray must contain Float32 or Float64 for vectors")
            }
        }
        _ => anyhow::bail!("Unsupported data type: {:?}", array.data_type())
    }
}

fn evaluate_binary_op(op: &BinaryOperator, left: &Value, right: &Value) -> Result<Value> {
    match op {
        BinaryOperator::Add => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a + b)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(*a as f64 + b)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a + *b as f64)),
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                _ => anyhow::bail!("Invalid operands for addition")
            }
        }
        BinaryOperator::Subtract => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(*a as f64 - b)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a - *b as f64)),
                _ => anyhow::bail!("Invalid operands for subtraction")
            }
        }
        BinaryOperator::Multiply => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a * b)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(*a as f64 * b)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a * *b as f64)),
                _ => anyhow::bail!("Invalid operands for multiplication")
            }
        }
        BinaryOperator::Divide => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => {
                    if *b == 0 {
                        anyhow::bail!("Division by zero")
                    }
                    Ok(Value::Float64(*a as f64 / *b as f64))
                }
                (Value::Float64(a), Value::Float64(b)) => {
                    if *b == 0.0 {
                        anyhow::bail!("Division by zero")
                    }
                    Ok(Value::Float64(a / b))
                }
                (Value::Int64(a), Value::Float64(b)) => {
                    if *b == 0.0 {
                        anyhow::bail!("Division by zero")
                    }
                    Ok(Value::Float64(*a as f64 / b))
                }
                (Value::Float64(a), Value::Int64(b)) => {
                    if *b == 0 {
                        anyhow::bail!("Division by zero")
                    }
                    Ok(Value::Float64(a / *b as f64))
                }
                _ => anyhow::bail!("Invalid operands for division")
            }
        }
        BinaryOperator::Modulo => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => {
                    if *b == 0 {
                        anyhow::bail!("Modulo by zero")
                    }
                    Ok(Value::Int64(a % b))
                }
                (Value::Float64(a), Value::Float64(b)) => {
                    if *b == 0.0 {
                        anyhow::bail!("Modulo by zero")
                    }
                    Ok(Value::Float64(a % b))
                }
                _ => anyhow::bail!("Invalid operands for modulo")
            }
        }
        BinaryOperator::Eq => {
            // Handle numeric comparisons with type coercion
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64((a == b) as i64)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Int64((a == b) as i64)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Int64((*a as f64 == *b) as i64)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Int64((*a == *b as f64) as i64)),
                _ => Ok(Value::Int64((left == right) as i64)),
            }
        }
        BinaryOperator::Ne => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64((a != b) as i64)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Int64((a != b) as i64)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Int64((*a as f64 != *b) as i64)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Int64((*a != *b as f64) as i64)),
                _ => Ok(Value::Int64((left != right) as i64)),
            }
        }
        BinaryOperator::Lt => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64((a < b) as i64)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Int64((a < b) as i64)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Int64(((*a as f64) < *b) as i64)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Int64((*a < (*b as f64)) as i64)),
                _ => anyhow::bail!("Cannot compare non-numeric values with <"),
            }
        }
        BinaryOperator::Le => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64((a <= b) as i64)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Int64((a <= b) as i64)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Int64(((*a as f64) <= *b) as i64)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Int64((*a <= (*b as f64)) as i64)),
                _ => anyhow::bail!("Cannot compare non-numeric values with <="),
            }
        }
        BinaryOperator::Gt => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64((a > b) as i64)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Int64((a > b) as i64)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Int64(((*a as f64) > *b) as i64)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Int64((*a > (*b as f64)) as i64)),
                _ => anyhow::bail!("Cannot compare non-numeric values with >"),
            }
        }
        BinaryOperator::Ge => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64((a >= b) as i64)),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Int64((a >= b) as i64)),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Int64(((*a as f64) >= *b) as i64)),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Int64((*a >= (*b as f64)) as i64)),
                _ => anyhow::bail!("Cannot compare non-numeric values with >="),
            }
        }
        BinaryOperator::And => {
            let left_bool = match left {
                Value::Int64(0) | Value::Null => false,
                _ => true,
            };
            let right_bool = match right {
                Value::Int64(0) | Value::Null => false,
                _ => true,
            };
            Ok(Value::Int64((left_bool && right_bool) as i64))
        }
        BinaryOperator::Or => {
            let left_bool = match left {
                Value::Int64(0) | Value::Null => false,
                _ => true,
            };
            let right_bool = match right {
                Value::Int64(0) | Value::Null => false,
                _ => true,
            };
            Ok(Value::Int64((left_bool || right_bool) as i64))
        }
        BinaryOperator::Concat => {
            let left_str = format!("{}", left);
            let right_str = format!("{}", right);
            Ok(Value::String(format!("{}{}", left_str, right_str)))
        }
        BinaryOperator::Like => {
            // Simple LIKE implementation (supports % and _)
            let pattern = match right {
                Value::String(s) => s,
                _ => return Ok(Value::Int64(0)),
            };
            let text = match left {
                Value::String(s) => s,
                _ => return Ok(Value::Int64(0)),
            };
            Ok(Value::Int64(like_match(text, pattern) as i64))
        }
        BinaryOperator::ILike => {
            // Case-insensitive LIKE
            let pattern = match right {
                Value::String(s) => s.to_lowercase(),
                _ => return Ok(Value::Int64(0)),
            };
            let text = match left {
                Value::String(s) => s.to_lowercase(),
                _ => return Ok(Value::Int64(0)),
            };
            Ok(Value::Int64(like_match(&text, &pattern) as i64))
        }
        BinaryOperator::Regexp => {
            // TODO: Implement regex matching
            anyhow::bail!("REGEXP not yet implemented")
        }
        BinaryOperator::In | BinaryOperator::NotIn => {
            anyhow::bail!("IN operator should be handled by Expression::In")
        }
        BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseOr | BinaryOperator::BitwiseXor => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => {
                    let result = match op {
                        BinaryOperator::BitwiseAnd => a & b,
                        BinaryOperator::BitwiseOr => a | b,
                        BinaryOperator::BitwiseXor => a ^ b,
                        _ => unreachable!(),
                    };
                    Ok(Value::Int64(result))
                }
                _ => anyhow::bail!("Bitwise operations require integer operands")
            }
        }
        BinaryOperator::Power => {
            match (left, right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Float64((*a as f64).powf(*b as f64))),
                (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a.powf(*b))),
                (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64((*a as f64).powf(*b))),
                (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a.powf(*b as f64))),
                _ => anyhow::bail!("Invalid operands for power")
            }
        }
    }
}

fn evaluate_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value> {
    match op {
        UnaryOperator::Not => {
            let bool_val = match val {
                Value::Int64(0) | Value::Null => true,
                _ => false,
            };
            Ok(Value::Int64(bool_val as i64))
        }
        UnaryOperator::Negate => {
            match val {
                Value::Int64(i) => Ok(Value::Int64(-i)),
                Value::Float64(f) => Ok(Value::Float64(-f)),
                _ => anyhow::bail!("Cannot negate non-numeric value")
            }
        }
        UnaryOperator::Plus => Ok(val.clone()),
        UnaryOperator::BitwiseNot => {
            match val {
                Value::Int64(i) => Ok(Value::Int64(!i)),
                _ => anyhow::bail!("Bitwise NOT requires integer operand")
            }
        }
    }
}

fn evaluate_function(name: &str, args: &[Value]) -> Result<Value> {
    let func_name = name.to_uppercase();
    
    match func_name.as_str() {
        // String functions
        "CONCAT" | "||" => {
            Ok(Value::String(args.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join("")))
        }
        "SUBSTRING" | "SUBSTR" => {
            if args.len() < 2 {
                anyhow::bail!("SUBSTRING requires at least 2 arguments");
            }
            let text = match &args[0] {
                Value::String(s) => s,
                _ => anyhow::bail!("SUBSTRING first argument must be string")
            };
            let start = match &args[1] {
                Value::Int64(i) => *i as usize,
                _ => anyhow::bail!("SUBSTRING second argument must be integer")
            };
            let len = if args.len() > 2 {
                match &args[2] {
                    Value::Int64(i) => Some(*i as usize),
                    _ => anyhow::bail!("SUBSTRING third argument must be integer")
                }
            } else {
                None
            };
            let start_idx = if start > 0 { start - 1 } else { 0 };
            let result = if let Some(l) = len {
                text.chars().skip(start_idx).take(l).collect()
            } else {
                text.chars().skip(start_idx).collect()
            };
            Ok(Value::String(result))
        }
        "LENGTH" | "LEN" => {
            let text = match &args[0] {
                Value::String(s) => s,
                _ => anyhow::bail!("LENGTH argument must be string")
            };
            Ok(Value::Int64(text.len() as i64))
        }
        "UPPER" | "UCASE" => {
            let text = match &args[0] {
                Value::String(s) => s.to_uppercase(),
                _ => anyhow::bail!("UPPER argument must be string")
            };
            Ok(Value::String(text))
        }
        "LOWER" | "LCASE" => {
            let text = match &args[0] {
                Value::String(s) => s.to_lowercase(),
                _ => anyhow::bail!("LOWER argument must be string")
            };
            Ok(Value::String(text))
        }
        "TRIM" => {
            let text = match &args[0] {
                Value::String(s) => s.trim().to_string(),
                _ => anyhow::bail!("TRIM argument must be string")
            };
            Ok(Value::String(text))
        }
        "LTRIM" => {
            let text = match &args[0] {
                Value::String(s) => s.trim_start().to_string(),
                _ => anyhow::bail!("LTRIM argument must be string")
            };
            Ok(Value::String(text))
        }
        "RTRIM" => {
            let text = match &args[0] {
                Value::String(s) => s.trim_end().to_string(),
                _ => anyhow::bail!("RTRIM argument must be string")
            };
            Ok(Value::String(text))
        }
        "REPLACE" => {
            if args.len() < 3 {
                anyhow::bail!("REPLACE requires 3 arguments");
            }
            let text = match &args[0] {
                Value::String(s) => s,
                _ => anyhow::bail!("REPLACE first argument must be string")
            };
            let from = match &args[1] {
                Value::String(s) => s,
                _ => anyhow::bail!("REPLACE second argument must be string")
            };
            let to = match &args[2] {
                Value::String(s) => s,
                _ => anyhow::bail!("REPLACE third argument must be string")
            };
            Ok(Value::String(text.replace(from, to)))
        }
        
        // Numeric functions
        "ABS" => {
            match &args[0] {
                Value::Int64(i) => Ok(Value::Int64(i.abs())),
                Value::Float64(f) => Ok(Value::Float64(f.abs())),
                _ => anyhow::bail!("ABS argument must be numeric")
            }
        }
        "ROUND" => {
            let val = match &args[0] {
                Value::Int64(i) => *i as f64,
                Value::Float64(f) => *f,
                _ => anyhow::bail!("ROUND argument must be numeric")
            };
            let decimals = if args.len() > 1 {
                match &args[1] {
                    Value::Int64(i) => *i,
                    _ => 0,
                }
            } else {
                0
            };
            Ok(Value::Float64((val * 10_f64.powi(decimals as i32)).round() / 10_f64.powi(decimals as i32)))
        }
        "FLOOR" => {
            match &args[0] {
                Value::Int64(i) => Ok(Value::Int64(*i)),
                Value::Float64(f) => Ok(Value::Float64(f.floor())),
                _ => anyhow::bail!("FLOOR argument must be numeric")
            }
        }
        "CEIL" | "CEILING" => {
            match &args[0] {
                Value::Int64(i) => Ok(Value::Int64(*i)),
                Value::Float64(f) => Ok(Value::Float64(f.ceil())),
                _ => anyhow::bail!("CEIL argument must be numeric")
            }
        }
        "SQRT" => {
            let val = match &args[0] {
                Value::Int64(i) => *i as f64,
                Value::Float64(f) => *f,
                _ => anyhow::bail!("SQRT argument must be numeric")
            };
            if val < 0.0 {
                anyhow::bail!("SQRT of negative number")
            }
            Ok(Value::Float64(val.sqrt()))
        }
        "POWER" | "POW" => {
            let base = match &args[0] {
                Value::Int64(i) => *i as f64,
                Value::Float64(f) => *f,
                _ => anyhow::bail!("POWER base must be numeric")
            };
            let exp = match &args[1] {
                Value::Int64(i) => *i as f64,
                Value::Float64(f) => *f,
                _ => anyhow::bail!("POWER exponent must be numeric")
            };
            Ok(Value::Float64(base.powf(exp)))
        }
        "MOD" => {
            let a = match &args[0] {
                Value::Int64(i) => *i,
                Value::Float64(f) => *f as i64,
                _ => anyhow::bail!("MOD argument must be numeric")
            };
            let b = match &args[1] {
                Value::Int64(i) => *i,
                Value::Float64(f) => *f as i64,
                _ => anyhow::bail!("MOD argument must be numeric")
            };
            if b == 0 {
                anyhow::bail!("Modulo by zero")
            }
            Ok(Value::Int64(a % b))
        }
        
        // Conditional functions
        "COALESCE" => {
            for arg in args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" | "ISNULL" => {
            if args.is_empty() {
                anyhow::bail!("IFNULL requires at least 1 argument");
            }
            if matches!(&args[0], Value::Null) {
                if args.len() > 1 {
                    Ok(args[1].clone())
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(args[0].clone())
            }
        }
        "NULLIF" => {
            if args.len() < 2 {
                anyhow::bail!("NULLIF requires 2 arguments");
            }
            if args[0] == args[1] {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "GREATEST" => {
            if args.is_empty() {
                anyhow::bail!("GREATEST requires at least 1 argument");
            }
            let mut max_val = &args[0];
            for arg in &args[1..] {
                if arg > max_val {
                    max_val = arg;
                }
            }
            Ok(max_val.clone())
        }
        "LEAST" => {
            if args.is_empty() {
                anyhow::bail!("LEAST requires at least 1 argument");
            }
            let mut min_val = &args[0];
            for arg in &args[1..] {
                if arg < min_val {
                    min_val = arg;
                }
            }
            Ok(min_val.clone())
        }
        
        // Type conversion
        "CAST" => {
            if args.len() < 2 {
                anyhow::bail!("CAST requires 2 arguments");
            }
            // CAST is handled by Expression::Cast, this is fallback
            Ok(args[0].clone())
        }
        
        // Vector functions
        "VECTOR_SIMILARITY" => {
            if args.len() != 2 {
                anyhow::bail!("VECTOR_SIMILARITY requires exactly 2 arguments");
            }
            let vec1 = match &args[0] {
                Value::Vector(v) => v,
                _ => anyhow::bail!("VECTOR_SIMILARITY first argument must be a vector")
            };
            let vec2 = match &args[1] {
                Value::Vector(v) => v,
                _ => anyhow::bail!("VECTOR_SIMILARITY second argument must be a vector")
            };
            
            if vec1.len() != vec2.len() {
                anyhow::bail!("Vectors must have the same dimension for VECTOR_SIMILARITY");
            }
            
            // Calculate cosine similarity
            let similarity = crate::storage::vector_index::cosine_similarity_impl(vec1, vec2);
            Ok(Value::Float64(similarity as f64))
        }
        "VECTOR_DISTANCE" => {
            if args.len() != 2 {
                anyhow::bail!("VECTOR_DISTANCE requires exactly 2 arguments");
            }
            let vec1 = match &args[0] {
                Value::Vector(v) => v,
                _ => anyhow::bail!("VECTOR_DISTANCE first argument must be a vector")
            };
            let vec2 = match &args[1] {
                Value::Vector(v) => v,
                _ => anyhow::bail!("VECTOR_DISTANCE second argument must be a vector")
            };
            
            if vec1.len() != vec2.len() {
                anyhow::bail!("Vectors must have the same dimension for VECTOR_DISTANCE");
            }
            
            // Calculate L2 (Euclidean) distance
            let distance = crate::storage::vector_index::l2_distance_impl(vec1, vec2);
            Ok(Value::Float64(distance as f64))
        }
        
        _ => anyhow::bail!("Unknown function: {}", name)
    }
}

fn evaluate_case(
    operand: Option<&Expression>,
    conditions: &[(Expression, Expression)],
    else_result: Option<&Expression>,
    evaluator: &ExpressionEvaluator,
    batch: &ExecutionBatch,
    row_idx: usize,
) -> Result<Value> {
    evaluate_case_with_context(operand, conditions, else_result, evaluator, batch, row_idx, Some(batch))
}

fn evaluate_case_with_context(
    operand: Option<&Expression>,
    conditions: &[(Expression, Expression)],
    else_result: Option<&Expression>,
    evaluator: &ExpressionEvaluator,
    batch: &ExecutionBatch,
    row_idx: usize,
    outer_context: Option<&ExecutionBatch>,
) -> Result<Value> {
    for (condition, result) in conditions {
        let condition_val = if let Some(op) = operand {
            // CASE expr WHEN val1 THEN ... WHEN val2 THEN ...
            let op_val = evaluator.evaluate_with_context(op, batch, row_idx, outer_context)?;
            let cond_val = evaluator.evaluate_with_context(condition, batch, row_idx, outer_context)?;
            op_val == cond_val
        } else {
            // CASE WHEN condition1 THEN ... WHEN condition2 THEN ...
            let cond_val = evaluator.evaluate_with_context(condition, batch, row_idx, outer_context)?;
            matches!(cond_val, Value::Int64(1) | Value::Int64(-1)) || !matches!(cond_val, Value::Int64(0) | Value::Null)
        };
        
        if condition_val {
            return evaluator.evaluate_with_context(result, batch, row_idx, outer_context);
        }
    }
    
    if let Some(else_expr) = else_result {
        evaluator.evaluate_with_context(else_expr, batch, row_idx, outer_context)
    } else {
        Ok(Value::Null)
    }
}

pub fn cast_value(val: &Value, target_type: &DataType) -> Result<Value> {
    match target_type {
        // Integer types
        DataType::Int8 => {
            match val {
                Value::Int64(i) => Ok(Value::Int32(*i as i32)),
                Value::Int32(i) => Ok(Value::Int32(*i)),
                Value::Float64(f) => Ok(Value::Int32(*f as i32)),
                Value::Float32(f) => Ok(Value::Int32(*f as i32)),
                Value::String(s) => Ok(Value::Int32(s.parse::<i32>().unwrap_or(0))),
                Value::Bool(b) => Ok(Value::Int32(if *b { 1 } else { 0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to integer
                Value::Null => Ok(Value::Null),
            }
        }
        DataType::Int16 => {
            match val {
                Value::Int64(i) => Ok(Value::Int32(*i as i32)),
                Value::Int32(i) => Ok(Value::Int32(*i)),
                Value::Float64(f) => Ok(Value::Int32(*f as i32)),
                Value::Float32(f) => Ok(Value::Int32(*f as i32)),
                Value::String(s) => Ok(Value::Int32(s.parse::<i32>().unwrap_or(0))),
                Value::Bool(b) => Ok(Value::Int32(if *b { 1 } else { 0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to integer
                Value::Null => Ok(Value::Null),
            }
        }
        DataType::Int32 => {
            match val {
                Value::Int64(i) => Ok(Value::Int32(*i as i32)),
                Value::Int32(i) => Ok(Value::Int32(*i)),
                Value::Float64(f) => Ok(Value::Int32(*f as i32)),
                Value::Float32(f) => Ok(Value::Int32(*f as i32)),
                Value::String(s) => Ok(Value::Int32(s.parse().unwrap_or(0))),
                Value::Bool(b) => Ok(Value::Int32(if *b { 1 } else { 0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to integer
                Value::Null => Ok(Value::Null),
            }
        }
        DataType::Int64 => {
            match val {
                Value::Int64(i) => Ok(Value::Int64(*i)),
                Value::Int32(i) => Ok(Value::Int64(*i as i64)),
                Value::Float64(f) => Ok(Value::Int64(*f as i64)),
                Value::Float32(f) => Ok(Value::Int64(*f as i64)),
                Value::String(s) => Ok(Value::Int64(s.parse().unwrap_or(0))),
                Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to integer
                Value::Null => Ok(Value::Null),
            }
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            // Unsigned integers - convert to signed Int64 (best we can do with Value enum)
            match val {
                Value::Int64(i) => Ok(Value::Int64(*i.max(&0))),
                Value::Int32(i) => Ok(Value::Int64((*i as i64).max(0))),
                Value::Float64(f) => Ok(Value::Int64((*f as i64).max(0))),
                Value::Float32(f) => Ok(Value::Int64((*f as i64).max(0))),
                Value::String(s) => Ok(Value::Int64(s.parse::<i64>().unwrap_or(0).max(0))),
                Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to integer
                Value::Null => Ok(Value::Null),
            }
        }
        // Floating point types
        DataType::Float32 => {
            match val {
                Value::Int64(i) => Ok(Value::Float32(*i as f32)),
                Value::Int32(i) => Ok(Value::Float32(*i as f32)),
                Value::Float64(f) => Ok(Value::Float32(*f as f32)),
                Value::Float32(f) => Ok(Value::Float32(*f)),
                Value::String(s) => Ok(Value::Float32(s.parse().unwrap_or(0.0))),
                Value::Bool(b) => Ok(Value::Float32(if *b { 1.0 } else { 0.0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to float
                Value::Null => Ok(Value::Null),
            }
        }
        DataType::Float64 => {
            match val {
                Value::Int64(i) => Ok(Value::Float64(*i as f64)),
                Value::Int32(i) => Ok(Value::Float64(*i as f64)),
                Value::Float64(f) => Ok(Value::Float64(*f)),
                Value::Float32(f) => Ok(Value::Float64(*f as f64)),
                Value::String(s) => Ok(Value::Float64(s.parse().unwrap_or(0.0))),
                Value::Bool(b) => Ok(Value::Float64(if *b { 1.0 } else { 0.0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to float
                Value::Null => Ok(Value::Null),
            }
        }
        // String types
        DataType::Utf8 | DataType::LargeUtf8 => {
            Ok(Value::String(format!("{}", val)))
        }
        // Boolean
        DataType::Boolean => {
            match val {
                Value::Int64(i) => Ok(Value::Bool(*i != 0)),
                Value::Int32(i) => Ok(Value::Bool(*i != 0)),
                Value::Float64(f) => Ok(Value::Bool(*f != 0.0)),
                Value::Float32(f) => Ok(Value::Bool(*f != 0.0)),
                Value::String(s) => {
                    let s_lower = s.to_lowercase();
                    Ok(Value::Bool(s_lower == "true" || s_lower == "1" || s_lower == "yes" || s_lower == "t"))
                }
                Value::Bool(b) => Ok(Value::Bool(*b)),
                Value::Vector(_) => Ok(Value::Bool(true)), // Non-empty vector is truthy
                Value::Null => Ok(Value::Null),
            }
        }
        // Date/Time types - convert to string representation
        DataType::Date32 | DataType::Date64 | 
        DataType::Timestamp(_, _) | DataType::Time32(_) | DataType::Time64(_) |
        DataType::Duration(_) | DataType::Interval(_) => {
            Ok(Value::String(format!("{}", val)))
        }
        // Decimal types - convert to Float64
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            match val {
                Value::Int64(i) => Ok(Value::Float64(*i as f64)),
                Value::Int32(i) => Ok(Value::Float64(*i as f64)),
                Value::Float64(f) => Ok(Value::Float64(*f)),
                Value::Float32(f) => Ok(Value::Float64(*f as f64)),
                Value::String(s) => Ok(Value::Float64(s.parse().unwrap_or(0.0))),
                Value::Bool(b) => Ok(Value::Float64(if *b { 1.0 } else { 0.0 })),
                Value::Vector(_) => Ok(Value::Null), // Cannot cast vector to decimal
                Value::Null => Ok(Value::Null),
            }
        }
        // Binary types - convert to string
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Ok(Value::String(format!("{:?}", val)))
        }
        // Other types - convert to string
        _ => {
            Ok(Value::String(format!("{}", val)))
        }
    }
}

fn like_match(text: &str, pattern: &str) -> bool {
    // Simple LIKE implementation: % matches any sequence, _ matches single character
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    
    fn match_pattern(text: &[char], pattern: &[char], text_idx: usize, pattern_idx: usize) -> bool {
        if pattern_idx >= pattern.len() {
            return text_idx >= text.len();
        }
        
        match pattern[pattern_idx] {
            '%' => {
                // Match zero or more characters
                if pattern_idx + 1 >= pattern.len() {
                    return true; // % at end matches everything
                }
                // Try matching zero or more characters
                for i in text_idx..=text.len() {
                    if match_pattern(text, pattern, i, pattern_idx + 1) {
                        return true;
                    }
                }
                false
            }
            '_' => {
                // Match single character
                if text_idx >= text.len() {
                    return false;
                }
                match_pattern(text, pattern, text_idx + 1, pattern_idx + 1)
            }
            c => {
                // Match exact character
                if text_idx >= text.len() || text[text_idx] != c {
                    return false;
                }
                match_pattern(text, pattern, text_idx + 1, pattern_idx + 1)
            }
        }
    }
    
    match_pattern(&text_chars, &pattern_chars, 0, 0)
}

