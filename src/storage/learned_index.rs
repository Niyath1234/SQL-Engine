/// Learned Indexes - Using ML models to predict data positions
/// Based on "The Case for Learned Index Structures" (Kraska et al., 2018)
use crate::storage::fragment::{ColumnFragment, Value};
use std::collections::HashMap;
use ordered_float::OrderedFloat;

/// Learned index that predicts position of a value
pub trait LearnedIndex {
    /// Predict the position of a value (returns approximate position)
    fn predict_position(&self, value: &Value) -> usize;
    
    /// Get the error bound for predictions
    fn error_bound(&self) -> usize;
    
    /// Search for a value using the learned index
    fn search(&self, value: &Value, fragment: &ColumnFragment) -> Option<usize>;
}

/// Recursive Model Index (RMI) - hierarchical learned index
#[derive(Clone)]
pub struct RecursiveModelIndex {
    /// Root model
    root_model: LinearModel,
    
    /// Second-level models (one per root prediction)
    second_level_models: Vec<LinearModel>,
    
    /// Error bound for predictions
    error_bound: usize,
    
    /// Total number of elements
    total_elements: usize,
}

/// Simple linear regression model for position prediction
#[derive(Clone)]
pub struct LinearModel {
    /// Slope (weight)
    slope: f64,
    
    /// Intercept (bias)
    intercept: f64,
    
    /// Min value used for training
    min_value: f64,
    
    /// Max value used for training
    max_value: f64,
}

impl LinearModel {
    /// Train a linear model from data
    fn train(values: &[f64], positions: &[usize]) -> Self {
        if values.is_empty() || positions.is_empty() {
            return Self {
                slope: 0.0,
                intercept: 0.0,
                min_value: 0.0,
                max_value: 0.0,
            };
        }
        
        let n = values.len() as f64;
        let sum_x: f64 = values.iter().sum();
        let sum_y: f64 = positions.iter().sum::<usize>() as f64;
        let sum_xy: f64 = values.iter().zip(positions.iter()).map(|(x, y)| x * (*y as f64)).sum();
        let sum_x2: f64 = values.iter().map(|x| x * x).sum();
        
        let slope = if sum_x2 == 0.0 {
            0.0
        } else {
            (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        };
        
        let intercept = (sum_y - slope * sum_x) / n;
        
        let min_value = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_value = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        Self {
            slope,
            intercept,
            min_value,
            max_value,
        }
    }
    
    /// Predict position for a value
    fn predict(&self, value: f64) -> f64 {
        self.slope * value + self.intercept
    }
}

impl RecursiveModelIndex {
    /// Build a learned index from a sorted fragment
    pub fn build(fragment: &ColumnFragment) -> Self {
        // Extract values and positions
        let mut values = Vec::new();
        let mut positions = Vec::new();
        
        // Sample values from fragment (for efficiency, sample every Nth element)
        let sample_rate = (fragment.len() / 1000).max(1);
        for i in (0..fragment.len()).step_by(sample_rate) {
            if let Some(value) = Self::extract_numeric_value(fragment, i) {
                values.push(value);
                positions.push(i);
            }
        }
        
        if values.is_empty() {
            return Self {
                root_model: LinearModel {
                    slope: 0.0,
                    intercept: 0.0,
                    min_value: 0.0,
                    max_value: 0.0,
                },
                second_level_models: vec![],
                error_bound: fragment.len(),
                total_elements: fragment.len(),
            };
        }
        
        // Train root model
        let root_model = LinearModel::train(&values, &positions);
        
        // Build second-level models (simplified: use 10 second-level models)
        let num_second_level = 10.min(fragment.len() / 100);
        let mut second_level_models = Vec::new();
        
        if num_second_level > 1 {
            let chunk_size = values.len() / num_second_level;
            for chunk_idx in 0..num_second_level {
                let start = chunk_idx * chunk_size;
                let end = if chunk_idx == num_second_level - 1 {
                    values.len()
                } else {
                    (chunk_idx + 1) * chunk_size
                };
                
                if start < end {
                    let chunk_values = &values[start..end];
                    let chunk_positions = &positions[start..end];
                    let model = LinearModel::train(chunk_values, chunk_positions);
                    second_level_models.push(model);
                }
            }
        }
        
        // Calculate error bound (max deviation from predicted position)
        let mut max_error = 0;
        for (i, &value) in values.iter().enumerate() {
            let predicted = root_model.predict(value) as usize;
            let actual = positions[i];
            let error = if predicted > actual {
                predicted - actual
            } else {
                actual - predicted
            };
            max_error = max_error.max(error);
        }
        
        // Add some safety margin
        let error_bound = (max_error * 2).max(fragment.len() / 100);
        
        Self {
            root_model,
            second_level_models,
            error_bound,
            total_elements: fragment.len(),
        }
    }
    
    /// Extract numeric value from fragment at given index
    fn extract_numeric_value(fragment: &ColumnFragment, idx: usize) -> Option<f64> {
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return None;
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|arr| arr.value(idx) as f64)
            }
            arrow::datatypes::DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .map(|arr| arr.value(idx))
            }
            _ => None,
        }
    }
}

impl LearnedIndex for RecursiveModelIndex {
    fn predict_position(&self, value: &Value) -> usize {
        let numeric_value = match value {
            Value::Int64(v) => *v as f64,
            Value::Int32(v) => *v as f64,
            Value::Float64(v) => *v,
            Value::Float32(v) => *v as f64,
            Value::Vector(_) | Value::String(_) | Value::Bool(_) | Value::Null => return 0,
        };
        
        let predicted = self.root_model.predict(numeric_value);
        
        // Use second-level model if available
        if !self.second_level_models.is_empty() {
            let model_idx = (predicted as usize * self.second_level_models.len()) / self.total_elements;
            let model_idx = model_idx.min(self.second_level_models.len() - 1);
            if let Some(model) = self.second_level_models.get(model_idx) {
                let refined = model.predict(numeric_value);
                return refined.max(0.0) as usize;
            }
        }
        
        predicted.max(0.0) as usize
    }
    
    fn error_bound(&self) -> usize {
        self.error_bound
    }
    
    fn search(&self, value: &Value, fragment: &ColumnFragment) -> Option<usize> {
        let predicted_pos = self.predict_position(value);
        let error_bound = self.error_bound();
        
        // Search in the error bound range
        let start = predicted_pos.saturating_sub(error_bound);
        let end = (predicted_pos + error_bound).min(fragment.len());
        
        // Linear search in the bounded range
        for i in start..end {
            if let Some(fragment_value) = Self::extract_value_at(fragment, i) {
                if Self::values_equal(value, &fragment_value) {
                    return Some(i);
                }
            }
        }
        
        None
    }
}

impl RecursiveModelIndex {
    fn extract_value_at(fragment: &ColumnFragment, idx: usize) -> Option<Value> {
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return Some(Value::Null);
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|arr| Value::Int64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .map(|arr| Value::Float64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Utf8 => {
                array.as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .map(|arr| Value::String(arr.value(idx).to_string()))
            }
            _ => None,
        }
    }
    
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Int64(x), Value::Int64(y)) => x == y,
            (Value::Float64(x), Value::Float64(y)) => {
                OrderedFloat(*x) == OrderedFloat(*y)
            }
            (Value::String(x), Value::String(y)) => x == y,
            (Value::Vector(x), Value::Vector(y)) => x == y,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

/// Hash-based learned index for equality lookups
pub struct HashLearnedIndex {
    /// Hash map from value to position
    hash_map: HashMap<u64, Vec<usize>>,
    
    /// Hash function
    hash_fn: fn(&Value) -> u64,
}

impl HashLearnedIndex {
    /// Build a hash-based learned index
    pub fn build(fragment: &ColumnFragment) -> Self {
        let mut hash_map = HashMap::new();
        
        for i in 0..fragment.len() {
            if let Some(value) = Self::extract_value(fragment, i) {
                let hash = Self::hash_value(&value);
                hash_map.entry(hash).or_insert_with(Vec::new).push(i);
            }
        }
        
        Self {
            hash_map,
            hash_fn: Self::hash_value,
        }
    }
    
    fn extract_value(fragment: &ColumnFragment, idx: usize) -> Option<Value> {
        let array = fragment.get_array()?;
        if array.is_null(idx) {
            return Some(Value::Null);
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|arr| Value::Int64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Float64 => {
                array.as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .map(|arr| Value::Float64(arr.value(idx)))
            }
            arrow::datatypes::DataType::Utf8 => {
                array.as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .map(|arr| Value::String(arr.value(idx).to_string()))
            }
            _ => None,
        }
    }
    
    fn hash_value(value: &Value) -> u64 {
        use fxhash::FxHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = FxHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Lookup positions for a value
    pub fn lookup(&self, value: &Value) -> Vec<usize> {
        let hash = (self.hash_fn)(value);
        self.hash_map.get(&hash).cloned().unwrap_or_default()
    }
}

impl LearnedIndex for HashLearnedIndex {
    fn predict_position(&self, value: &Value) -> usize {
        let positions = self.lookup(value);
        positions.first().copied().unwrap_or(0)
    }
    
    fn error_bound(&self) -> usize {
        0 // Hash index gives exact positions
    }
    
    fn search(&self, value: &Value, _fragment: &ColumnFragment) -> Option<usize> {
        self.lookup(value).first().copied()
    }
}

