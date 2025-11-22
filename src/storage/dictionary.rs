use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use fxhash::FxHashMap;
use crate::storage::fragment::Value;

/// Dictionary-encoded column fragment
/// Stores values as integer codes (u16 or u32) with a separate dictionary
#[derive(Clone, Debug)]
pub struct DictionaryEncodedFragment {
    /// Integer codes (u16 for < 65536 distinct values, u32 otherwise)
    pub codes: Arc<dyn Array>,
    
    /// Dictionary: code -> original value
    pub dictionary: Vec<Value>,
    
    /// Reverse lookup: value -> code (for encoding)
    pub reverse_dict: FxHashMap<DictValue, u32>,
    
    /// Number of rows
    pub row_count: usize,
    
    /// Original data type
    pub original_type: DataType,
}

/// Value type for dictionary (simplified from fragment::Value)
/// Note: Float64 is not Hash/Eq, so we use a workaround
#[derive(Clone, Debug)]
pub enum DictValue {
    Int64(i64),
    Int32(i32),
    Float64(u64), // Store as bits for hashing
    String(String),
    Bool(bool),
    Null,
}

impl PartialEq for DictValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DictValue::Int64(a), DictValue::Int64(b)) => a == b,
            (DictValue::Int32(a), DictValue::Int32(b)) => a == b,
            (DictValue::Float64(a), DictValue::Float64(b)) => a == b,
            (DictValue::String(a), DictValue::String(b)) => a == b,
            (DictValue::Bool(a), DictValue::Bool(b)) => a == b,
            (DictValue::Null, DictValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for DictValue {}

impl std::hash::Hash for DictValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            DictValue::Int64(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            DictValue::Int32(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            DictValue::Float64(bits) => {
                2u8.hash(state);
                bits.hash(state);
            }
            DictValue::String(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            DictValue::Bool(b) => {
                4u8.hash(state);
                b.hash(state);
            }
            DictValue::Null => {
                5u8.hash(state);
            }
        }
    }
}

impl From<&crate::storage::fragment::Value> for DictValue {
    fn from(v: &crate::storage::fragment::Value) -> Self {
        match v {
            crate::storage::fragment::Value::Int64(x) => DictValue::Int64(*x),
            crate::storage::fragment::Value::Int32(x) => DictValue::Int32(*x),
            crate::storage::fragment::Value::Float64(x) => DictValue::Float64(x.to_bits()),
            crate::storage::fragment::Value::Float32(x) => DictValue::Float64((*x as f64).to_bits()),
            crate::storage::fragment::Value::String(s) => DictValue::String(s.clone()),
            crate::storage::fragment::Value::Bool(b) => DictValue::Bool(*b),
            crate::storage::fragment::Value::Vector(_) => DictValue::Null, // Vectors not supported in dictionary encoding yet
            crate::storage::fragment::Value::Null => DictValue::Null,
        }
    }
}

impl DictionaryEncodedFragment {
    /// Create dictionary-encoded fragment from an Arrow array
    pub fn from_array(array: &Arc<dyn Array>) -> Result<Self, anyhow::Error> {
        let row_count = array.len();
        let mut dictionary: Vec<DictValue> = Vec::new();
        let mut reverse_dict: FxHashMap<DictValue, u32> = FxHashMap::default();
        let mut codes: Vec<u32> = Vec::with_capacity(row_count);
        
        // Build dictionary by scanning array
        for i in 0..row_count {
            let value = extract_dict_value(array, i)?;
            let code = *reverse_dict.entry(value.clone()).or_insert_with(|| {
                let code = dictionary.len() as u32;
                dictionary.push(value);
                code
            });
            codes.push(code);
        }
        
        // Convert dictionary to fragment::Value for compatibility
        let dict_values: Vec<crate::storage::fragment::Value> = dictionary.iter().map(|v| {
            match v {
                DictValue::Int64(x) => crate::storage::fragment::Value::Int64(*x),
                DictValue::Int32(x) => crate::storage::fragment::Value::Int32(*x),
                DictValue::Float64(bits) => crate::storage::fragment::Value::Float64(f64::from_bits(*bits)),
                DictValue::String(s) => crate::storage::fragment::Value::String(s.clone()),
                DictValue::Bool(b) => crate::storage::fragment::Value::Bool(*b),
                DictValue::Null => crate::storage::fragment::Value::Null,
            }
        }).collect();
        
        // Choose code type based on dictionary size
        let codes_array: Arc<dyn Array> = if dictionary.len() < 65536 {
            // Use u16 codes
            let codes_u16: Vec<u16> = codes.iter().map(|&c| c as u16).collect();
            Arc::new(UInt16Array::from(codes_u16))
        } else {
            // Use u32 codes
            Arc::new(UInt32Array::from(codes))
        };
        
        Ok(Self {
            codes: codes_array,
            dictionary: dict_values,
            reverse_dict: reverse_dict,
            row_count,
            original_type: array.data_type().clone(),
        })
    }
    
    /// Decode a single value by code
    pub fn decode(&self, code: u32) -> Option<&crate::storage::fragment::Value> {
        self.dictionary.get(code as usize)
    }
    
    /// Get code at a specific row index
    pub fn get_code(&self, idx: usize) -> Option<u32> {
        match self.codes.data_type() {
            DataType::UInt16 => {
                let arr = self.codes.as_any().downcast_ref::<UInt16Array>()?;
                if idx < arr.len() && !arr.is_null(idx) {
                    Some(arr.value(idx) as u32)
                } else {
                    None
                }
            }
            DataType::UInt32 => {
                let arr = self.codes.as_any().downcast_ref::<UInt32Array>()?;
                if idx < arr.len() && !arr.is_null(idx) {
                    Some(arr.value(idx))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    
    /// Decode entire fragment back to original array (for compatibility)
    pub fn decode_to_array(&self) -> Result<Arc<dyn Array>, anyhow::Error> {
        match &self.original_type {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let mut values: Vec<Option<String>> = Vec::with_capacity(self.row_count);
                for i in 0..self.row_count {
                    if let Some(code) = self.get_code(i) {
                        if let Some(val) = self.decode(code) {
                            values.push(Some(format!("{}", val)));
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
                Ok(Arc::new(StringArray::from(values)))
            }
            DataType::Int64 => {
                let mut values: Vec<Option<i64>> = Vec::with_capacity(self.row_count);
                for i in 0..self.row_count {
                    if let Some(code) = self.get_code(i) {
                        if let Some(val) = self.decode(code) {
                            if let crate::storage::fragment::Value::Int64(x) = val {
                                values.push(Some(*x));
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
                Ok(Arc::new(Int64Array::from(values)))
            }
            DataType::Int32 => {
                let mut values: Vec<Option<i32>> = Vec::with_capacity(self.row_count);
                for i in 0..self.row_count {
                    if let Some(code) = self.get_code(i) {
                        if let Some(val) = self.decode(code) {
                            if let crate::storage::fragment::Value::Int32(x) = val {
                                values.push(Some(*x));
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
                Ok(Arc::new(Int32Array::from(values)))
            }
            DataType::Float64 => {
                let mut values: Vec<Option<f64>> = Vec::with_capacity(self.row_count);
                for i in 0..self.row_count {
                    if let Some(code) = self.get_code(i) {
                        if let Some(val) = self.decode(code) {
                            if let crate::storage::fragment::Value::Float64(x) = val {
                                values.push(Some(*x));
                            } else {
                                values.push(None);
                            }
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
                Ok(Arc::new(Float64Array::from(values)))
            }
            _ => {
                // Default to string representation
                let mut values: Vec<Option<String>> = Vec::with_capacity(self.row_count);
                for i in 0..self.row_count {
                    if let Some(code) = self.get_code(i) {
                        if let Some(val) = self.decode(code) {
                            values.push(Some(format!("{}", val)));
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }
                Ok(Arc::new(StringArray::from(values)))
            }
        }
    }
}

/// Extract a dictionary value from an array at a specific index
fn extract_dict_value(array: &Arc<dyn Array>, idx: usize) -> Result<DictValue, anyhow::Error> {
    if array.is_null(idx) {
        return Ok(DictValue::Null);
    }
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            Ok(DictValue::Int64(arr.value(idx)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
            Ok(DictValue::Int32(arr.value(idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            Ok(DictValue::Float64(arr.value(idx).to_bits()))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float32Array"))?;
            Ok(DictValue::Float64((arr.value(idx) as f64).to_bits()))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
            Ok(DictValue::String(arr.value(idx).to_string()))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to BooleanArray"))?;
            Ok(DictValue::Bool(arr.value(idx)))
        }
        _ => {
            // Default: convert to string
            Ok(DictValue::String(format!("{:?}", array)))
        }
    }
}

/// Check if a column is a good candidate for dictionary encoding
/// Returns true if cardinality is low (< 65536 distinct values)
pub fn should_dictionary_encode(array: &Arc<dyn Array>, max_cardinality: usize) -> bool {
    // Quick heuristic: if array is small or we can quickly estimate cardinality
    if array.len() < 1000 {
        return true; // Small arrays are always good candidates
    }
    
    // For larger arrays, we'd need to scan to count distinct values
    // For now, use a simple heuristic: if it's a string array with reasonable size
    matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8)
}

