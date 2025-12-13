/// Phase 5: Automatic Compression Selection
/// 
/// Automatically selects the best compression method for a column fragment
/// based on data characteristics (cardinality, sortedness, value range, etc.)

use crate::storage::fragment::{ColumnFragment, CompressionType};
use crate::storage::compression::compress_fragment;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;

/// Compression strategy configuration
#[derive(Clone, Debug)]
pub struct CompressionStrategy {
    /// Minimum compression ratio to apply (e.g., 2.0 = must compress to <50% of original)
    pub min_compression_ratio: f64,
    
    /// Enable dictionary compression
    pub enable_dictionary: bool,
    
    /// Enable delta encoding
    pub enable_delta: bool,
    
    /// Enable bit-packing
    pub enable_bitpack: bool,
    
    /// Enable RLE
    pub enable_rle: bool,
    
    /// Enable LZ4
    pub enable_lz4: bool,
    
    /// Enable Zstd
    pub enable_zstd: bool,
}

impl Default for CompressionStrategy {
    fn default() -> Self {
        Self {
            min_compression_ratio: 1.5, // Must compress to <67% of original
            enable_dictionary: true,
            enable_delta: true,
            enable_bitpack: true,
            enable_rle: true,
            enable_lz4: true,
            enable_zstd: true,
        }
    }
}

/// Automatically select and apply the best compression for a fragment
pub fn auto_compress(fragment: &ColumnFragment, strategy: &CompressionStrategy) -> Result<ColumnFragment> {
    // If already compressed, return as-is
    if fragment.metadata.compression != CompressionType::None {
        return Ok(fragment.clone());
    }
    
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // Try compression methods in order of preference (fastest/least CPU first)
    let candidates = vec![
        // 1. Dictionary (very fast, great for low-cardinality)
        if strategy.enable_dictionary {
            try_compress(fragment, CompressionType::Dictionary, strategy)?
        } else {
            None
        },
        
        // 2. RLE (very fast, great for repeated values)
        if strategy.enable_rle {
            try_compress(fragment, CompressionType::RLE, strategy)?
        } else {
            None
        },
        
        // 3. Delta (fast, great for sorted data)
        if strategy.enable_delta {
            try_compress(fragment, CompressionType::Delta, strategy)?
        } else {
            None
        },
        
        // 4. Bit-packing (fast, great for small integers)
        if strategy.enable_bitpack {
            try_compress(fragment, CompressionType::BitPacked, strategy)?
        } else {
            None
        },
        
        // 5. LZ4 (fast compression, good general-purpose)
        if strategy.enable_lz4 {
            try_compress(fragment, CompressionType::Lz4, strategy)?
        } else {
            None
        },
        
        // 6. Zstd (slower but better compression)
        if strategy.enable_zstd {
            try_compress(fragment, CompressionType::Zstd, strategy)?
        } else {
            None
        },
    ];
    
    // Select best compression (highest ratio)
    let mut best: Option<(ColumnFragment, f64)> = None;
    let original_size = fragment.metadata.memory_size;
    
    for candidate in candidates.into_iter().flatten() {
        let compressed_size = candidate.metadata.memory_size;
        if compressed_size > 0 {
            let ratio = original_size as f64 / compressed_size as f64;
            if ratio >= strategy.min_compression_ratio {
                if let Some((_, best_ratio)) = &best {
                    if ratio > *best_ratio {
                        best = Some((candidate, ratio));
                    }
                } else {
                    best = Some((candidate, ratio));
                }
            }
        }
    }
    
    // Return best compressed version, or original if none met threshold
    Ok(best.map(|(f, _)| f).unwrap_or_else(|| fragment.clone()))
}

/// Try compressing with a specific method and return if successful
fn try_compress(
    fragment: &ColumnFragment,
    compression_type: CompressionType,
    strategy: &CompressionStrategy,
) -> Result<Option<ColumnFragment>> {
    match compress_fragment(fragment, compression_type) {
        Ok(compressed) => {
            let original_size = fragment.metadata.memory_size;
            let compressed_size = compressed.metadata.memory_size;
            
            if compressed_size > 0 && original_size > 0 {
                let ratio = original_size as f64 / compressed_size as f64;
                if ratio >= strategy.min_compression_ratio {
                    return Ok(Some(compressed));
                }
            }
            Ok(None)
        }
        Err(_) => Ok(None), // Compression failed, skip this method
    }
}

/// Analyze fragment to suggest best compression method
pub fn suggest_compression(fragment: &ColumnFragment) -> CompressionType {
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return CompressionType::None,
    };
    
    // Check cardinality for dictionary encoding
    if let Some(cardinality) = estimate_cardinality(array) {
        if cardinality < 65536 && cardinality < array.len() / 10 {
            // Low cardinality (< 10% of rows are distinct) → dictionary
            return CompressionType::Dictionary;
        }
    }
    
    // Check if sorted for delta encoding
    if is_sorted(array) {
        return CompressionType::Delta;
    }
    
    // Check value range for bit-packing
    if let Some((min, max)) = get_value_range(array) {
        if let Some(bits) = bits_needed_for_range(min, max) {
            if bits < 32 {
                // Can pack into fewer bits → bit-packing
                return CompressionType::BitPacked;
            }
        }
    }
    
    // Check for repeated values (RLE)
    if has_many_repeats(array) {
        return CompressionType::RLE;
    }
    
    // Default: LZ4 (fast, general-purpose)
    CompressionType::Lz4
}

/// Estimate cardinality (number of distinct values)
fn estimate_cardinality(array: &Arc<dyn Array>) -> Option<usize> {
    // Simple heuristic: sample first 1000 values
    let sample_size = array.len().min(1000);
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            let mut distinct = std::collections::HashSet::<i64>::new();
            for i in 0..sample_size {
                if !arr.is_null(i) {
                    distinct.insert(arr.value(i));
                }
            }
            // Extrapolate to full array
            let sample_ratio = sample_size as f64 / array.len() as f64;
            Some((distinct.len() as f64 / sample_ratio.max(0.01)) as usize)
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            let mut distinct = std::collections::HashSet::<String>::new();
            for i in 0..sample_size {
                if !arr.is_null(i) {
                    distinct.insert(arr.value(i).to_string());
                }
            }
            // Extrapolate to full array
            let sample_ratio = sample_size as f64 / array.len() as f64;
            Some((distinct.len() as f64 / sample_ratio.max(0.01)) as usize)
        }
        _ => None,
    }
}

/// Check if array is sorted
fn is_sorted(array: &Arc<dyn Array>) -> bool {
    match array.data_type() {
        DataType::Int64 => {
            let arr = match array.as_any().downcast_ref::<Int64Array>() {
                Some(a) => a,
                None => return false,
            };
            for i in 1..arr.len().min(1000) {
                if !arr.is_null(i) && !arr.is_null(i - 1) {
                    if arr.value(i) < arr.value(i - 1) {
                        return false;
                    }
                }
            }
            true
        }
        DataType::Float64 => {
            let arr = match array.as_any().downcast_ref::<Float64Array>() {
                Some(a) => a,
                None => return false,
            };
            for i in 1..arr.len().min(1000) {
                if !arr.is_null(i) && !arr.is_null(i - 1) {
                    if arr.value(i) < arr.value(i - 1) {
                        return false;
                    }
                }
            }
            true
        }
        _ => false,
    }
}

/// Get min/max value range
fn get_value_range(array: &Arc<dyn Array>) -> Option<(i64, i64)> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for i in 0..arr.len().min(10000) {
                if !arr.is_null(i) {
                    let v = arr.value(i);
                    min = min.min(v);
                    max = max.max(v);
                }
            }
            if min <= max {
                Some((min, max))
            } else {
                None
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()?;
            let mut min = i32::MAX;
            let mut max = i32::MIN;
            for i in 0..arr.len().min(10000) {
                if !arr.is_null(i) {
                    let v = arr.value(i);
                    min = min.min(v);
                    max = max.max(v);
                }
            }
            if min <= max {
                Some((min as i64, max as i64))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Calculate bits needed to represent values in range [min, max]
fn bits_needed_for_range(min: i64, max: i64) -> Option<u32> {
    let range = max.wrapping_sub(min) as u64;
    if range == 0 {
        return Some(1);
    }
    Some(64 - range.leading_zeros())
}

/// Check if array has many repeated values (good for RLE)
fn has_many_repeats(array: &Arc<dyn Array>) -> bool {
    match array.data_type() {
        DataType::Int64 => {
            let arr = match array.as_any().downcast_ref::<Int64Array>() {
                Some(a) => a,
                None => return false,
            };
            let sample_size = arr.len().min(1000);
            let mut consecutive_repeats = 0;
            let mut max_consecutive = 0;
            let mut last_value = None;
            
            for i in 0..sample_size {
                if !arr.is_null(i) {
                    let v = arr.value(i);
                    if Some(v) == last_value {
                        consecutive_repeats += 1;
                        max_consecutive = max_consecutive.max(consecutive_repeats);
                    } else {
                        consecutive_repeats = 1;
                        last_value = Some(v);
                    }
                }
            }
            
            // RLE is good if we have runs of 3+ consecutive values
            max_consecutive >= 3
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_suggest_compression() {
        // Low cardinality → dictionary
        let low_card = Arc::new(Int64Array::from(vec![1, 2, 1, 2, 1, 2])) as Arc<dyn Array>;
        let fragment = ColumnFragment {
            array: Some(low_card),
            ..Default::default()
        };
        assert_eq!(suggest_compression(&fragment), CompressionType::Dictionary);
    }
}

