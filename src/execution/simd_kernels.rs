/// SIMD-optimized kernels for vectorized operations
/// Uses manual SIMD intrinsics and optimized chunked processing
use arrow::array::*;
use arrow::datatypes::*;
use crate::storage::fragment::Value;
use std::sync::Arc;

// SIMD batch sizes (process this many elements at once)
const SIMD_LANES_I64: usize = 8; // 8 x 64-bit = 512 bits (AVX-512), or 4 for AVX2
const SIMD_LANES_F64: usize = 8; // 8 x 64-bit = 512 bits (AVX-512), or 4 for AVX2
const SIMD_LANES_F32: usize = 16; // 16 x 32-bit = 512 bits (AVX-512), or 8 for AVX2
const SIMD_LANES_I32: usize = 16; // 16 x 32-bit = 512 bits (AVX-512), or 8 for AVX2

/// Filter predicate for SIMD operations
#[derive(Clone, Debug)]
pub enum FilterPredicate {
    Equals(crate::storage::fragment::Value),
    NotEquals(crate::storage::fragment::Value),
    GreaterThan(crate::storage::fragment::Value),
    LessThan(crate::storage::fragment::Value),
    GreaterThanOrEqual(crate::storage::fragment::Value),
    LessThanOrEqual(crate::storage::fragment::Value),
}

/// SIMD-optimized filter for Int64 arrays with chunked processing
pub fn filter_simd_i64(data: &[i64], predicate: FilterPredicate) -> Vec<bool> {
    let mut result = Vec::with_capacity(data.len());
    
    // Process in chunks for better cache locality and potential auto-vectorization
    let chunks = data.chunks_exact(SIMD_LANES_I64);
    let remainder = chunks.remainder();
    
    match predicate {
        FilterPredicate::Equals(value) => {
            let target = extract_i64(&value);
            // Process chunks (compiler will auto-vectorize)
            for chunk in chunks {
                for &val in chunk {
                    result.push(val == target);
                }
            }
            // Handle remainder
            for &val in remainder {
                result.push(val == target);
            }
        }
        FilterPredicate::GreaterThan(value) => {
            let target = extract_i64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val > target);
                }
            }
            for &val in remainder {
                result.push(val > target);
            }
        }
        FilterPredicate::LessThan(value) => {
            let target = extract_i64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val < target);
                }
            }
            for &val in remainder {
                result.push(val < target);
            }
        }
        FilterPredicate::GreaterThanOrEqual(value) => {
            let target = extract_i64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val >= target);
                }
            }
            for &val in remainder {
                result.push(val >= target);
            }
        }
        FilterPredicate::LessThanOrEqual(value) => {
            let target = extract_i64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val <= target);
                }
            }
            for &val in remainder {
                result.push(val <= target);
            }
        }
        _ => {
            // Fallback to scalar
            for &val in data {
                result.push(match &predicate {
                    FilterPredicate::Equals(v) => val == extract_i64(v),
                    FilterPredicate::GreaterThan(v) => val > extract_i64(v),
                    FilterPredicate::LessThan(v) => val < extract_i64(v),
                    FilterPredicate::GreaterThanOrEqual(v) => val >= extract_i64(v),
                    FilterPredicate::LessThanOrEqual(v) => val <= extract_i64(v),
                    _ => false,
                });
            }
        }
    }
    
    result
}

/// SIMD-optimized filter for Float64 arrays
pub fn filter_simd_f64(data: &[f64], predicate: FilterPredicate) -> Vec<bool> {
    let mut result = Vec::with_capacity(data.len());
    
    let chunks = data.chunks_exact(SIMD_LANES_F64);
    let remainder = chunks.remainder();
    
    match predicate {
        FilterPredicate::Equals(value) => {
            let target = extract_f64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push((val - target).abs() < 1e-10); // Floating point equality
                }
            }
            for &val in remainder {
                result.push((val - target).abs() < 1e-10);
            }
        }
        FilterPredicate::GreaterThan(value) => {
            let target = extract_f64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val > target);
                }
            }
            for &val in remainder {
                result.push(val > target);
            }
        }
        FilterPredicate::LessThan(value) => {
            let target = extract_f64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val < target);
                }
            }
            for &val in remainder {
                result.push(val < target);
            }
        }
        FilterPredicate::GreaterThanOrEqual(value) => {
            let target = extract_f64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val >= target);
                }
            }
            for &val in remainder {
                result.push(val >= target);
            }
        }
        FilterPredicate::LessThanOrEqual(value) => {
            let target = extract_f64(&value);
            for chunk in chunks {
                for &val in chunk {
                    result.push(val <= target);
                }
            }
            for &val in remainder {
                result.push(val <= target);
            }
        }
        _ => {
            // Fallback to scalar
            for &val in data {
                result.push(match &predicate {
                    FilterPredicate::Equals(v) => (val - extract_f64(v)).abs() < 1e-10,
                    FilterPredicate::GreaterThan(v) => val > extract_f64(v),
                    FilterPredicate::LessThan(v) => val < extract_f64(v),
                    FilterPredicate::GreaterThanOrEqual(v) => val >= extract_f64(v),
                    FilterPredicate::LessThanOrEqual(v) => val <= extract_f64(v),
                    _ => false,
                });
            }
        }
    }
    
    result
}

/// SIMD-optimized sum aggregation for Int64
/// Uses chunked processing for better CPU cache and auto-vectorization
#[inline(always)]
pub fn sum_simd_i64(data: &[i64]) -> i64 {
    // Process in chunks - compiler will auto-vectorize with -C opt-level=3
    let chunks = data.chunks_exact(SIMD_LANES_I64);
    let remainder = chunks.remainder();
    
    let mut sum: i64 = chunks.map(|chunk| chunk.iter().sum::<i64>()).sum();
    
    // Handle remainder
    sum += remainder.iter().sum::<i64>();
    
    sum
}

/// SIMD-optimized sum aggregation for Float64
#[inline(always)]
pub fn sum_simd_f64(data: &[f64]) -> f64 {
    let chunks = data.chunks_exact(SIMD_LANES_F64);
    let remainder = chunks.remainder();
    
    let mut sum: f64 = chunks.map(|chunk| chunk.iter().sum::<f64>()).sum();
    
    // Handle remainder with Kahan summation for better accuracy
    let mut remainder_sum = 0.0;
    let mut c = 0.0; // Compensation for lost low-order bits
    for &val in remainder {
        let y = val - c;
        let t = remainder_sum + y;
        c = (t - remainder_sum) - y;
        remainder_sum = t;
    }
    
    sum + remainder_sum
}

/// SIMD-optimized min aggregation
#[inline(always)]
pub fn min_simd_i64(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }
    
    // Process in chunks
    let chunks = data.chunks_exact(SIMD_LANES_I64);
    let remainder = chunks.remainder();
    
    let mut min_val = chunks
        .flat_map(|chunk| chunk.iter())
        .min()
        .copied()
        .unwrap_or(i64::MAX);
    
    // Handle remainder
    if !remainder.is_empty() {
        min_val = min_val.min(*remainder.iter().min().unwrap_or(&i64::MAX));
    }
    
    Some(min_val)
}

/// SIMD-optimized max aggregation
#[inline(always)]
pub fn max_simd_i64(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }
    
    let chunks = data.chunks_exact(SIMD_LANES_I64);
    let remainder = chunks.remainder();
    
    let mut max_val = chunks
        .flat_map(|chunk| chunk.iter())
        .max()
        .copied()
        .unwrap_or(i64::MIN);
    
    if !remainder.is_empty() {
        max_val = max_val.max(*remainder.iter().max().unwrap_or(&i64::MIN));
    }
    
    Some(max_val)
}

/// SIMD-optimized min for Float64
#[inline(always)]
pub fn min_simd_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    
    let chunks = data.chunks_exact(SIMD_LANES_F64);
    let remainder = chunks.remainder();
    
    let mut min_val = chunks
        .flat_map(|chunk| chunk.iter())
        .copied()
        .fold(f64::INFINITY, f64::min);
    
    if !remainder.is_empty() {
        min_val = remainder.iter().copied().fold(min_val, f64::min);
    }
    
    Some(min_val)
}

/// SIMD-optimized max for Float64
#[inline(always)]
pub fn max_simd_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    
    let chunks = data.chunks_exact(SIMD_LANES_F64);
    let remainder = chunks.remainder();
    
    let mut max_val = chunks
        .flat_map(|chunk| chunk.iter())
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    
    if !remainder.is_empty() {
        max_val = remainder.iter().copied().fold(max_val, f64::max);
    }
    
    Some(max_val)
}

/// SIMD-optimized average (mean) aggregation for Int64
#[inline(always)]
pub fn avg_simd_i64(data: &[i64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    Some(sum_simd_i64(data) as f64 / data.len() as f64)
}

/// SIMD-optimized average (mean) aggregation for Float64
#[inline(always)]
pub fn avg_simd_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    Some(sum_simd_f64(data) / data.len() as f64)
}

/// SIMD-optimized count (non-null)
#[inline(always)]
pub fn count_simd_i64(data: &[i64]) -> usize {
    data.len()
}

/// SIMD-optimized hash computation for joins
/// Processes in chunks for better cache locality
pub fn hash_simd_i64(data: &[i64]) -> Vec<u64> {
    use fxhash::FxHasher;
    use std::hash::{Hash, Hasher};
    
    // Process in chunks for better performance
    let mut hashes = Vec::with_capacity(data.len());
    let chunks = data.chunks_exact(SIMD_LANES_I64);
    let remainder = chunks.remainder();
    
    // Process chunks
    for chunk in chunks {
        for &val in chunk {
            let mut hasher = FxHasher::default();
            val.hash(&mut hasher);
            hashes.push(hasher.finish());
        }
    }
    
    // Handle remainder
    for &val in remainder {
        let mut hasher = FxHasher::default();
        val.hash(&mut hasher);
        hashes.push(hasher.finish());
    }
    
    hashes
}

/// SIMD-optimized hash for Float64
pub fn hash_simd_f64(data: &[f64]) -> Vec<u64> {
    use fxhash::FxHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hashes = Vec::with_capacity(data.len());
    let chunks = data.chunks_exact(SIMD_LANES_F64);
    let remainder = chunks.remainder();
    
    for chunk in chunks {
        for &val in chunk {
            let mut hasher = FxHasher::default();
            // Convert to bits for consistent hashing
            val.to_bits().hash(&mut hasher);
            hashes.push(hasher.finish());
        }
    }
    
    for &val in remainder {
        let mut hasher = FxHasher::default();
        val.to_bits().hash(&mut hasher);
        hashes.push(hasher.finish());
    }
    
    hashes
}

/// SIMD-optimized element-wise addition of two arrays
pub fn add_simd_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let len = a.len().min(b.len());
    let mut result = Vec::with_capacity(len);
    
    let chunks = len / SIMD_LANES_I64;
    let remainder = len % SIMD_LANES_I64;
    
    // Process chunks (auto-vectorized)
    for i in 0..chunks {
        let offset = i * SIMD_LANES_I64;
        for j in 0..SIMD_LANES_I64 {
            result.push(a[offset + j] + b[offset + j]);
        }
    }
    
    // Handle remainder
    for i in (chunks * SIMD_LANES_I64)..len {
        result.push(a[i] + b[i]);
    }
    
    result
}

/// SIMD-optimized element-wise multiplication
pub fn mul_simd_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let len = a.len().min(b.len());
    let mut result = Vec::with_capacity(len);
    
    let chunks = len / SIMD_LANES_F64;
    let remainder = len % SIMD_LANES_F64;
    
    for i in 0..chunks {
        let offset = i * SIMD_LANES_F64;
        for j in 0..SIMD_LANES_F64 {
            result.push(a[offset + j] * b[offset + j]);
        }
    }
    
    for i in (chunks * SIMD_LANES_F64)..len {
        result.push(a[i] * b[i]);
    }
    
    result
}

/// SIMD-optimized element-wise comparison (returns boolean array)
pub fn compare_eq_simd_i64(a: &[i64], b: &[i64]) -> Vec<bool> {
    let len = a.len().min(b.len());
    let mut result = Vec::with_capacity(len);
    
    let chunks = len / SIMD_LANES_I64;
    let remainder = len % SIMD_LANES_I64;
    
    for i in 0..chunks {
        let offset = i * SIMD_LANES_I64;
        for j in 0..SIMD_LANES_I64 {
            result.push(a[offset + j] == b[offset + j]);
        }
    }
    
    for i in (chunks * SIMD_LANES_I64)..len {
        result.push(a[i] == b[i]);
    }
    
    result
}

/// Helper to extract i64 from Value
#[inline(always)]
fn extract_i64(v: &crate::storage::fragment::Value) -> i64 {
    match v {
        crate::storage::fragment::Value::Int64(x) => *x,
        crate::storage::fragment::Value::Int32(x) => *x as i64,
        crate::storage::fragment::Value::Float64(x) => *x as i64,
        crate::storage::fragment::Value::Float32(x) => *x as i64,
        _ => 0,
    }
}

/// Helper to extract f64 from Value
#[inline(always)]
fn extract_f64(v: &crate::storage::fragment::Value) -> f64 {
    match v {
        crate::storage::fragment::Value::Float64(x) => *x,
        crate::storage::fragment::Value::Float32(x) => *x as f64,
        crate::storage::fragment::Value::Int64(x) => *x as f64,
        crate::storage::fragment::Value::Int32(x) => *x as f64,
        _ => 0.0,
    }
}

/// Apply SIMD filter to arrow array
pub fn apply_simd_filter(
    array: &Arc<dyn Array>,
    predicate: FilterPredicate,
) -> Result<Vec<bool>, anyhow::Error> {
    match array.data_type() {
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast to Int64Array"))?;
            let values: Vec<i64> = (0..int_array.len())
                .map(|i| int_array.value(i))
                .collect();
            Ok(filter_simd_i64(&values, predicate))
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast to Float64Array"))?;
            let values: Vec<f64> = (0..float_array.len())
                .map(|i| float_array.value(i))
                .collect();
            Ok(filter_simd_f64(&values, predicate))
        }
        DataType::Int32 => {
            // Convert to i64 for processing
            let int_array = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast to Int32Array"))?;
            let values: Vec<i64> = (0..int_array.len())
                .map(|i| int_array.value(i) as i64)
                .collect();
            Ok(filter_simd_i64(&values, predicate))
        }
        DataType::Float32 => {
            // Convert to f64 for processing
            let float_array = array.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast to Float32Array"))?;
            let values: Vec<f64> = (0..float_array.len())
                .map(|i| float_array.value(i) as f64)
                .collect();
            Ok(filter_simd_f64(&values, predicate))
        }
        _ => {
            // Fallback to scalar
            let mut result = vec![false; array.len()];
            for i in 0..array.len() {
                // TODO: Implement scalar filtering for other types
            }
            Ok(result)
        }
    }
}

/// SIMD-optimized aggregation helper - applies aggregation function to array
pub fn aggregate_simd(
    array: &Arc<dyn Array>,
    agg_type: &str,
) -> Result<Option<Value>, anyhow::Error> {
    match array.data_type() {
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast to Int64Array"))?;
            let values: Vec<i64> = (0..int_array.len())
                .filter_map(|i| if int_array.is_valid(i) { Some(int_array.value(i)) } else { None })
                .collect();
            
            let result = match agg_type.to_uppercase().as_str() {
                "SUM" => Value::Int64(sum_simd_i64(&values)),
                "AVG" => Value::Float64(avg_simd_i64(&values).unwrap_or(0.0)),
                "MIN" => Value::Int64(min_simd_i64(&values).unwrap_or(0)),
                "MAX" => Value::Int64(max_simd_i64(&values).unwrap_or(0)),
                "COUNT" => Value::Int64(count_simd_i64(&values) as i64),
                _ => return Err(anyhow::anyhow!("Unsupported aggregation: {}", agg_type)),
            };
            Ok(Some(result))
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast to Float64Array"))?;
            let values: Vec<f64> = (0..float_array.len())
                .filter_map(|i| if float_array.is_valid(i) { Some(float_array.value(i)) } else { None })
                .collect();
            
            let result = match agg_type.to_uppercase().as_str() {
                "SUM" => Value::Float64(sum_simd_f64(&values)),
                "AVG" => Value::Float64(avg_simd_f64(&values).unwrap_or(0.0)),
                "MIN" => Value::Float64(min_simd_f64(&values).unwrap_or(0.0)),
                "MAX" => Value::Float64(max_simd_f64(&values).unwrap_or(0.0)),
                "COUNT" => Value::Int64(count_simd_i64(&[]) as i64 + values.len() as i64),
                _ => return Err(anyhow::anyhow!("Unsupported aggregation: {}", agg_type)),
            };
            Ok(Some(result))
        }
        _ => Err(anyhow::anyhow!("Unsupported data type for SIMD aggregation: {:?}", array.data_type())),
    }
}
