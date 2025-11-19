/// Scalar Function Implementations
/// Individual implementations of scalar SQL functions
use crate::storage::fragment::Value;
use anyhow::Result;

/// Evaluate a scalar function
pub fn evaluate_scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    let func_name = name.to_uppercase();
    
    match func_name.as_str() {
        // String functions are in expression.rs
        // This module can be extended for more complex functions
        _ => anyhow::bail!("Unknown scalar function: {}", name)
    }
}

