/// SQL Function Registry
/// Central registry for all SQL functions (scalar, aggregate, window, table)
use crate::storage::fragment::Value;
use arrow::datatypes::*;
use anyhow::Result;

/// Function signature
#[derive(Clone, Debug)]
pub struct FunctionSignature {
    pub name: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub variadic: bool, // Can accept variable number of arguments
}

/// Function registry
pub struct FunctionRegistry {
    /// Scalar functions
    scalar_functions: Vec<FunctionSignature>,
    /// Aggregate functions
    aggregate_functions: Vec<FunctionSignature>,
    /// Window functions
    window_functions: Vec<FunctionSignature>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            scalar_functions: Vec::new(),
            aggregate_functions: Vec::new(),
            window_functions: Vec::new(),
        };
        
        // Register built-in functions
        registry.register_builtin_functions();
        
        registry
    }
    
    fn register_builtin_functions(&mut self) {
        // String functions
        self.register_scalar("CONCAT", vec![DataType::Utf8], DataType::Utf8, true);
        self.register_scalar("SUBSTRING", vec![DataType::Utf8, DataType::Int64], DataType::Utf8, false);
        self.register_scalar("SUBSTR", vec![DataType::Utf8, DataType::Int64], DataType::Utf8, false);
        self.register_scalar("LENGTH", vec![DataType::Utf8], DataType::Int64, false);
        self.register_scalar("LEN", vec![DataType::Utf8], DataType::Int64, false);
        self.register_scalar("UPPER", vec![DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("LOWER", vec![DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("TRIM", vec![DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("LTRIM", vec![DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("RTRIM", vec![DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("REPLACE", vec![DataType::Utf8, DataType::Utf8, DataType::Utf8], DataType::Utf8, false);
        
        // Numeric functions
        self.register_scalar("ABS", vec![DataType::Float64], DataType::Float64, false);
        self.register_scalar("ROUND", vec![DataType::Float64], DataType::Float64, false);
        self.register_scalar("FLOOR", vec![DataType::Float64], DataType::Float64, false);
        self.register_scalar("CEIL", vec![DataType::Float64], DataType::Float64, false);
        self.register_scalar("CEILING", vec![DataType::Float64], DataType::Float64, false);
        self.register_scalar("SQRT", vec![DataType::Float64], DataType::Float64, false);
        self.register_scalar("POWER", vec![DataType::Float64, DataType::Float64], DataType::Float64, false);
        self.register_scalar("POW", vec![DataType::Float64, DataType::Float64], DataType::Float64, false);
        self.register_scalar("MOD", vec![DataType::Int64, DataType::Int64], DataType::Int64, false);
        
        // Conditional functions
        self.register_scalar("COALESCE", vec![DataType::Utf8], DataType::Utf8, true);
        self.register_scalar("IFNULL", vec![DataType::Utf8, DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("NULLIF", vec![DataType::Utf8, DataType::Utf8], DataType::Utf8, false);
        self.register_scalar("GREATEST", vec![DataType::Float64], DataType::Float64, true);
        self.register_scalar("LEAST", vec![DataType::Float64], DataType::Float64, true);
        
        // Aggregate functions
        self.register_aggregate("COUNT", vec![DataType::Utf8], DataType::Int64, false);
        self.register_aggregate("SUM", vec![DataType::Float64], DataType::Float64, false);
        self.register_aggregate("AVG", vec![DataType::Float64], DataType::Float64, false);
        self.register_aggregate("MIN", vec![DataType::Float64], DataType::Float64, false);
        self.register_aggregate("MAX", vec![DataType::Float64], DataType::Float64, false);
        
        // Window functions
        self.register_window("ROW_NUMBER", vec![], DataType::Int64, false);
        self.register_window("RANK", vec![], DataType::Int64, false);
        self.register_window("DENSE_RANK", vec![], DataType::Int64, false);
    }
    
    fn register_scalar(&mut self, name: &str, arg_types: Vec<DataType>, return_type: DataType, variadic: bool) {
        self.scalar_functions.push(FunctionSignature {
            name: name.to_string(),
            arg_types,
            return_type,
            variadic,
        });
    }
    
    fn register_aggregate(&mut self, name: &str, arg_types: Vec<DataType>, return_type: DataType, variadic: bool) {
        self.aggregate_functions.push(FunctionSignature {
            name: name.to_string(),
            arg_types,
            return_type,
            variadic,
        });
    }
    
    fn register_window(&mut self, name: &str, arg_types: Vec<DataType>, return_type: DataType, variadic: bool) {
        self.window_functions.push(FunctionSignature {
            name: name.to_string(),
            arg_types,
            return_type,
            variadic,
        });
    }
    
    /// Find function signature
    pub fn find_function(&self, name: &str, arg_count: usize) -> Option<&FunctionSignature> {
        let name_upper = name.to_uppercase();
        
        // Search scalar functions
        for sig in &self.scalar_functions {
            if sig.name == name_upper {
                if sig.variadic || sig.arg_types.len() == arg_count {
                    return Some(sig);
                }
            }
        }
        
        // Search aggregate functions
        for sig in &self.aggregate_functions {
            if sig.name == name_upper {
                if sig.variadic || sig.arg_types.len() == arg_count {
                    return Some(sig);
                }
            }
        }
        
        // Search window functions
        for sig in &self.window_functions {
            if sig.name == name_upper {
                if sig.variadic || sig.arg_types.len() == arg_count {
                    return Some(sig);
                }
            }
        }
        
        None
    }
    
    /// Check if function is aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        let name_upper = name.to_uppercase();
        self.aggregate_functions.iter().any(|sig| sig.name == name_upper)
    }
    
    /// Check if function is window function
    pub fn is_window(&self, name: &str) -> bool {
        let name_upper = name.to_uppercase();
        self.window_functions.iter().any(|sig| sig.name == name_upper)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

