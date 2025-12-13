use crate::storage::fragment::{ColumnFragment, CompressionType};
use arrow::array::*;
use std::sync::Arc;

/// Compress a column fragment
pub fn compress_fragment(
    fragment: &ColumnFragment,
    compression_type: CompressionType,
) -> Result<ColumnFragment, anyhow::Error> {
    match compression_type {
        CompressionType::Dictionary => compress_dictionary(fragment),
        CompressionType::Delta => compress_delta(fragment),
        CompressionType::BitPacked => compress_bitpacked(fragment),
        CompressionType::RLE => compress_rle(fragment),
        CompressionType::Zstd => compress_zstd(fragment),
        CompressionType::Lz4 => compress_lz4(fragment),
        CompressionType::None => Ok(fragment.clone()),
    }
}

/// Dictionary encoding for low-cardinality columns
fn compress_dictionary(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use crate::storage::dictionary::DictionaryEncodedFragment;
    
    if let Some(ref array) = fragment.array {
        // Check if this is a good candidate for dictionary encoding
        if crate::storage::dictionary::should_dictionary_encode(array, 65536) {
            // Create dictionary-encoded fragment
            let dict_fragment = DictionaryEncodedFragment::from_array(array)?;
            
            // Phase 2: Store dictionary-encoded codes and dictionary
            let mut compressed = fragment.clone();
            compressed.array = Some(dict_fragment.codes.clone());
            compressed.metadata.compression = CompressionType::Dictionary;
            compressed.dictionary = Some(dict_fragment.dictionary);
            
            return Ok(compressed);
        }
    }
    
    // Not a good candidate or no array - return as-is
    Ok(fragment.clone())
}

/// Delta encoding for sorted columns
/// Stores differences between consecutive values (good for sorted/sequential data)
fn compress_delta(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // Delta encoding works best for numeric types
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            
            if arr.len() == 0 {
                return Ok(fragment.clone());
            }
            
            // Compute deltas (differences between consecutive values)
            let mut deltas = Vec::with_capacity(arr.len());
            let mut prev_value = 0i64;
            let mut first_value = 0i64;
            let mut has_first = false;
            
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    // Store a sentinel value for NULL (e.g., i64::MIN)
                    deltas.push(i64::MIN);
                } else {
                    let value = arr.value(i);
                    if !has_first {
                        first_value = value;
                        prev_value = value;
                        has_first = true;
                        deltas.push(value); // First value stored as-is
                    } else {
                        let delta = value.wrapping_sub(prev_value);
                        deltas.push(delta);
                        prev_value = value;
                    }
                }
            }
            
            // Create delta-encoded array (store deltas as Int64)
            let delta_array = Arc::new(Int64Array::from(deltas)) as Arc<dyn Array>;
            
            let mut compressed = fragment.clone();
            compressed.array = Some(delta_array);
            compressed.metadata.compression = CompressionType::Delta;
            // Store first value in min_value for reconstruction
            if has_first {
                compressed.metadata.min_value = Some(crate::storage::fragment::Value::Int64(first_value));
            }
            
            Ok(compressed)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            
            if arr.len() == 0 {
                return Ok(fragment.clone());
            }
            
            // Compute deltas for floats
            let mut deltas = Vec::with_capacity(arr.len());
            let mut prev_value = 0.0f64;
            let mut first_value = 0.0f64;
            let mut has_first = false;
            
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    deltas.push(f64::NAN); // Use NaN as sentinel for NULL
                } else {
                    let value = arr.value(i);
                    if !has_first {
                        first_value = value;
                        prev_value = value;
                        has_first = true;
                        deltas.push(value);
                    } else {
                        let delta = value - prev_value;
                        deltas.push(delta);
                        prev_value = value;
                    }
                }
            }
            
            let delta_array = Arc::new(Float64Array::from(deltas)) as Arc<dyn Array>;
            
            let mut compressed = fragment.clone();
            compressed.array = Some(delta_array);
            compressed.metadata.compression = CompressionType::Delta;
            if has_first {
                compressed.metadata.min_value = Some(crate::storage::fragment::Value::Float64(first_value));
            }
            
            Ok(compressed)
        }
        _ => {
            // Not a good candidate for delta encoding
            Ok(fragment.clone())
        }
    }
}

/// Bit-packing for integers
/// Packs integers into minimal bits needed (e.g., 0-255 → 8 bits instead of 64)
fn compress_bitpacked(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            
            if arr.len() == 0 {
                return Ok(fragment.clone());
            }
            
            // Find min and max to determine bit width needed
            let mut min_val = i64::MAX;
            let mut max_val = i64::MIN;
            let mut has_values = false;
            
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    min_val = min_val.min(val);
                    max_val = max_val.max(val);
                    has_values = true;
                }
            }
            
            if !has_values {
                return Ok(fragment.clone());
            }
            
            // Calculate range and bits needed
            let range = max_val.wrapping_sub(min_val) as u64;
            let bits_needed = if range == 0 {
                1
            } else {
                (64 - range.leading_zeros()) as usize
            };
            
            // Only bit-pack if it saves space (need at least 2× compression)
            if bits_needed >= 32 {
                return Ok(fragment.clone()); // Not worth it
            }
            
            // Pack values into bit-packed format
            // For simplicity, we'll use Int32Array if bits_needed <= 32
            // In production, we'd use a custom bit-packed format
            let mut packed_values = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    packed_values.push(None);
                } else {
                    let val = arr.value(i);
                    // Store offset from min (reduces range)
                    let offset = (val.wrapping_sub(min_val)) as i32;
                    packed_values.push(Some(offset));
                }
            }
            
            let packed_array = Arc::new(Int32Array::from(packed_values)) as Arc<dyn Array>;
            
            let mut compressed = fragment.clone();
            compressed.array = Some(packed_array);
            compressed.metadata.compression = CompressionType::BitPacked;
            compressed.metadata.metadata.insert("bitpack_min".to_string(), min_val.to_string());
            compressed.metadata.metadata.insert("bitpack_bits".to_string(), bits_needed.to_string());
            
            Ok(compressed)
        }
        DataType::Int32 => {
            // Similar logic for Int32
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
            
            if arr.len() == 0 {
                return Ok(fragment.clone());
            }
            
            let mut min_val = i32::MAX;
            let mut max_val = i32::MIN;
            let mut has_values = false;
            
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    min_val = min_val.min(val);
                    max_val = max_val.max(val);
                    has_values = true;
                }
            }
            
            if !has_values {
                return Ok(fragment.clone());
            }
            
            let range = (max_val.wrapping_sub(min_val)) as u32;
            let bits_needed = if range == 0 {
                1
            } else {
                (32 - range.leading_zeros()) as usize
            };
            
            if bits_needed >= 16 {
                return Ok(fragment.clone());
            }
            
            // Pack into Int16Array
            let mut packed_values = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    packed_values.push(None);
                } else {
                    let val = arr.value(i);
                    let offset = (val.wrapping_sub(min_val)) as i16;
                    packed_values.push(Some(offset));
                }
            }
            
            // Note: Arrow doesn't have Int16Array, so we'll keep as Int32 but mark as bit-packed
            // In production, we'd use a custom bit-packed format
            let packed_array = Arc::new(Int32Array::from(
                packed_values.iter().map(|v| v.map(|x| x as i32)).collect::<Vec<_>>()
            )) as Arc<dyn Array>;
            
            let mut compressed = fragment.clone();
            compressed.array = Some(packed_array);
            compressed.metadata.compression = CompressionType::BitPacked;
            compressed.metadata.metadata.insert("bitpack_min".to_string(), min_val.to_string());
            compressed.metadata.metadata.insert("bitpack_bits".to_string(), bits_needed.to_string());
            
            Ok(compressed)
        }
        _ => {
            Ok(fragment.clone())
        }
    }
}

/// Run-length encoding for repeated values
/// Stores (value, count) pairs - good for columns with many repeated values
fn compress_rle(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    use crate::storage::fragment::Value;
    
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // RLE works best for columns with many repeated consecutive values
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            
            if arr.len() == 0 {
                return Ok(fragment.clone());
            }
            
            // Build RLE encoding: (value, count) pairs
            let mut rle_pairs: Vec<(Option<i64>, usize)> = Vec::new();
            let mut current_value: Option<i64> = None;
            let mut current_count = 0;
            
            for i in 0..arr.len() {
                let value = if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                };
                
                if value == current_value {
                    current_count += 1;
                } else {
                    // Save previous run
                    if current_count > 0 {
                        rle_pairs.push((current_value, current_count));
                    }
                    // Start new run
                    current_value = value;
                    current_count = 1;
                }
            }
            
            // Save last run
            if current_count > 0 {
                rle_pairs.push((current_value, current_count));
            }
            
            // Only use RLE if it actually compresses (at least 2×)
            if rle_pairs.len() * 2 >= arr.len() {
                return Ok(fragment.clone()); // Not worth it
            }
            
            // Store RLE pairs: serialize to compressed_data
            // Format: [original_len: u64][count: u32][value: i64]...
            let mut rle_data = Vec::new();
            rle_data.extend_from_slice(&(arr.len() as u64).to_le_bytes());
            rle_data.extend_from_slice(&(rle_pairs.len() as u32).to_le_bytes());
            
            for (val, count) in &rle_pairs {
                rle_data.extend_from_slice(&count.to_le_bytes());
                if let Some(v) = val {
                    rle_data.push(1); // Not null
                    rle_data.extend_from_slice(&v.to_le_bytes());
                } else {
                    rle_data.push(0); // Null
                    rle_data.extend_from_slice(&0i64.to_le_bytes());
                }
            }
            
            let mut compressed = fragment.clone();
            compressed.metadata.compression = CompressionType::RLE;
            compressed.compressed_data = Some(rle_data);
            
            // Store values array for compatibility
            let values: Vec<Option<i64>> = rle_pairs.iter().map(|(v, _)| *v).collect();
            compressed.array = Some(Arc::new(Int64Array::from(values)) as Arc<dyn Array>);
            
            Ok(compressed)
        }
        DataType::Utf8 => {
            // RLE for strings (store repeated string values)
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
            
            if arr.len() == 0 {
                return Ok(fragment.clone());
            }
            
            let mut rle_pairs: Vec<(Option<String>, usize)> = Vec::new();
            let mut current_value: Option<String> = None;
            let mut current_count = 0;
            
            for i in 0..arr.len() {
                let value = if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                };
                
                if value == current_value {
                    current_count += 1;
                } else {
                    if current_count > 0 {
                        rle_pairs.push((current_value.clone(), current_count));
                    }
                    current_value = value;
                    current_count = 1;
                }
            }
            
            if current_count > 0 {
                rle_pairs.push((current_value, current_count));
            }
            
            if rle_pairs.len() * 2 >= arr.len() {
                return Ok(fragment.clone());
            }
            
            // Store RLE data: serialize to compressed_data
            let mut rle_data = Vec::new();
            rle_data.extend_from_slice(&(arr.len() as u64).to_le_bytes());
            rle_data.extend_from_slice(&(rle_pairs.len() as u32).to_le_bytes());
            
            for (val, count) in &rle_pairs {
                rle_data.extend_from_slice(&count.to_le_bytes());
                if let Some(v) = val {
                    rle_data.push(1); // Not null
                    let v_bytes = v.as_bytes();
                    rle_data.extend_from_slice(&(v_bytes.len() as u32).to_le_bytes());
                    rle_data.extend_from_slice(v_bytes);
                } else {
                    rle_data.push(0); // Null
                    rle_data.extend_from_slice(&0u32.to_le_bytes());
                }
            }
            
            let mut compressed = fragment.clone();
            compressed.metadata.compression = CompressionType::RLE;
            compressed.compressed_data = Some(rle_data);
            
            // Store first value array for compatibility
            let values: Vec<Option<String>> = rle_pairs.iter().map(|(v, _)| v.clone()).collect();
            let first_values: Vec<Option<&str>> = values.iter().map(|v| v.as_ref().map(|s| s.as_str())).collect();
            compressed.array = Some(Arc::new(StringArray::from(first_values)) as Arc<dyn Array>);
            
            Ok(compressed)
        }
        _ => Ok(fragment.clone()),
    }
}

/// Zstd compression
fn compress_zstd(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use zstd::encode_all;
    
    // If fragment doesn't have an array or is already compressed, return as-is
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // For now, use a simple serialization approach
    // In production, we'd use Arrow IPC format for better efficiency
    // Serialize array metadata and values to bytes
    let serialized = serialize_array_simple(array)?;
    
    // Compress with zstd (level 3 is a good balance of speed/compression)
    let compressed = encode_all(serialized.as_slice(), 3)?;
    
    // Create compressed fragment with array set to None
    let mut compressed_fragment = fragment.clone();
    compressed_fragment.array = None;
    compressed_fragment.metadata.compression = CompressionType::Zstd;
    compressed_fragment.metadata.memory_size = compressed.len();
    
    // Store compressed bytes in compressed_data field
    compressed_fragment.compressed_data = Some(compressed);
    
    Ok(compressed_fragment)
}

/// Simple serialization helper for arrays (temporary - should use Arrow IPC)
fn serialize_array_simple(array: &Arc<dyn Array>) -> Result<Vec<u8>, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    let mut bytes = Vec::new();
    
    // Store data type identifier (1 byte) so we can deserialize correctly
    let type_id: u8 = match array.data_type() {
        DataType::Int64 => 0,
        DataType::Int32 => 1,
        DataType::Float64 => 2,
        DataType::Float32 => 3,
        DataType::Utf8 => 4,
        DataType::LargeUtf8 => 5,
        DataType::Boolean => 6,
        _ => 255, // Unknown type
    };
    bytes.push(type_id);
    
    // Serialize based on data type
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            // Serialize null bitmap, values, and length
            bytes.extend_from_slice(&arr.len().to_le_bytes());
            for i in 0..arr.len() {
                bytes.push(if arr.is_null(i) { 0 } else { 1 });
                if !arr.is_null(i) {
                    bytes.extend_from_slice(&arr.value(i).to_le_bytes());
                }
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            bytes.extend_from_slice(&arr.len().to_le_bytes());
            for i in 0..arr.len() {
                bytes.push(if arr.is_null(i) { 0 } else { 1 });
                if !arr.is_null(i) {
                    bytes.extend_from_slice(&arr.value(i).to_le_bytes());
                }
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
            bytes.extend_from_slice(&arr.len().to_le_bytes());
            for i in 0..arr.len() {
                bytes.push(if arr.is_null(i) { 0 } else { 1 });
                if !arr.is_null(i) {
                    bytes.extend_from_slice(&arr.value(i).to_le_bytes());
                }
            }
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to BooleanArray"))?;
            bytes.extend_from_slice(&arr.len().to_le_bytes());
            for i in 0..arr.len() {
                bytes.push(if arr.is_null(i) { 0 } else { 1 });
                if !arr.is_null(i) {
                    bytes.push(if arr.value(i) { 1 } else { 0 });
                }
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to StringArray"))?;
            bytes.extend_from_slice(&arr.len().to_le_bytes());
            for i in 0..arr.len() {
                bytes.push(if arr.is_null(i) { 0 } else { 1 });
                if !arr.is_null(i) {
                    let val = arr.value(i);
                    bytes.extend_from_slice(&val.len().to_le_bytes());
                    bytes.extend_from_slice(val.as_bytes());
                }
            }
        }
        _ => {
            anyhow::bail!("Unsupported data type for compression: {:?}", array.data_type());
        }
    }
    
    Ok(bytes)
}

/// LZ4 compression
/// Fast compression algorithm, good balance of speed and compression ratio
fn compress_lz4(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use lz4::block::compress;
    
    // If fragment doesn't have an array or is already compressed, return as-is
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // Serialize array to bytes (reuse zstd serialization logic)
    let serialized = serialize_array_simple(array)?;
    
    // Compress with LZ4 (default compression level)
    let compressed = compress(&serialized, None, false)
        .map_err(|e| anyhow::anyhow!("LZ4 compression failed: {}", e))?;
    
    // Create compressed fragment
    let mut compressed_fragment = fragment.clone();
    compressed_fragment.array = None;
    compressed_fragment.metadata.compression = CompressionType::Lz4;
    compressed_fragment.metadata.memory_size = compressed.len();
    compressed_fragment.compressed_data = Some(compressed);
    
    Ok(compressed_fragment)
}

/// Decompress a column fragment
pub fn decompress_fragment(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    match fragment.metadata.compression {
        CompressionType::Dictionary => decompress_dictionary(fragment),
        CompressionType::Delta => decompress_delta(fragment),
        CompressionType::BitPacked => decompress_bitpacked(fragment),
        CompressionType::RLE => decompress_rle(fragment),
        CompressionType::Zstd => decompress_zstd(fragment),
        CompressionType::Lz4 => decompress_lz4(fragment),
        CompressionType::None => Ok(fragment.clone()),
    }
}

fn decompress_dictionary(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use crate::storage::dictionary::DictionaryEncodedFragment;
    
    if !matches!(fragment.metadata.compression, CompressionType::Dictionary) {
        return Ok(fragment.clone());
    }
    
    // Get dictionary and codes
    let dictionary = fragment.dictionary.as_ref()
        .ok_or_else(|| anyhow::anyhow!("Dictionary fragment missing dictionary"))?;
    
    let codes_array = fragment.array.as_ref()
        .ok_or_else(|| anyhow::anyhow!("Dictionary fragment missing codes array"))?;
    
    // Decode: map codes back to original values
    let codes = codes_array.as_any().downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow::anyhow!("Dictionary codes must be Int32Array"))?;
    
    // Reconstruct original array from codes and dictionary
    let mut values = Vec::with_capacity(codes.len());
    
    for i in 0..codes.len() {
        if codes.is_null(i) {
            values.push(None);
        } else {
            let code = codes.value(i) as usize;
            if code < dictionary.len() {
                // Get value from dictionary
                let value = &dictionary[code];
                values.push(Some(value.clone()));
            } else {
                anyhow::bail!("Dictionary code {} out of range (dictionary size: {})", code, dictionary.len());
            }
        }
    }
    
    // Reconstruct array based on dictionary value type
    if dictionary.is_empty() {
        return Ok(fragment.clone());
    }
    
    // Determine type from first dictionary value
    let reconstructed = match &dictionary[0] {
        crate::storage::fragment::Value::Int64(_) => {
            let ints: Vec<i64> = values.iter()
                .map(|v| match v {
                    Some(crate::storage::fragment::Value::Int64(x)) => *x,
                    _ => 0,
                })
                .collect();
            Arc::new(Int64Array::from(ints)) as Arc<dyn Array>
        }
        crate::storage::fragment::Value::String(_) => {
            let strings: Vec<&str> = values.iter()
                .map(|v| match v {
                    Some(crate::storage::fragment::Value::String(s)) => s.as_str(),
                    _ => "",
                })
                .collect();
            Arc::new(StringArray::from(strings)) as Arc<dyn Array>
        }
        _ => anyhow::bail!("Unsupported dictionary value type"),
    };
    
    let mut decompressed = fragment.clone();
    decompressed.array = Some(reconstructed);
    decompressed.metadata.compression = CompressionType::None;
    decompressed.dictionary = None;
    
    Ok(decompressed)
}

fn decompress_delta(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    if !matches!(fragment.metadata.compression, CompressionType::Delta) {
        return Ok(fragment.clone());
    }
    
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // Get first value from min_value metadata
    let first_value = match &fragment.metadata.min_value {
        Some(crate::storage::fragment::Value::Int64(v)) => *v,
        Some(crate::storage::fragment::Value::Float64(v)) => {
            // Handle float case separately
            return decompress_delta_float(fragment);
        }
        _ => anyhow::bail!("Delta compressed fragment missing first value in min_value"),
    };
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int64Array"))?;
            
            // Reconstruct original values from deltas
            let mut values = Vec::with_capacity(arr.len());
            let mut prev_value = first_value;
            
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(None);
                } else {
                    let delta = arr.value(i);
                    if delta == i64::MIN {
                        // Sentinel for NULL
                        values.push(None);
                    } else if i == 0 {
                        // First value
                        values.push(Some(first_value));
                        prev_value = first_value;
                    } else {
                        // Reconstruct: value = prev + delta
                        let value = prev_value.wrapping_add(delta);
                        values.push(Some(value));
                        prev_value = value;
                    }
                }
            }
            
            let reconstructed = Arc::new(Int64Array::from(values)) as Arc<dyn Array>;
            let mut decompressed = fragment.clone();
            decompressed.array = Some(reconstructed);
            decompressed.metadata.compression = CompressionType::None;
            
            Ok(decompressed)
        }
        _ => Ok(fragment.clone()),
    }
}

fn decompress_delta_float(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    let first_value = match &fragment.metadata.min_value {
        Some(crate::storage::fragment::Value::Float64(v)) => *v,
        _ => anyhow::bail!("Delta compressed fragment missing float first value"),
    };
    
    let array = fragment.array.as_ref()
        .ok_or_else(|| anyhow::anyhow!("Delta fragment missing array"))?;
    
    match array.data_type() {
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Float64Array"))?;
            
            let mut values = Vec::with_capacity(arr.len());
            let mut prev_value = first_value;
            
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(None);
                } else {
                    let delta = arr.value(i);
                    if delta.is_nan() {
                        values.push(None);
                    } else if i == 0 {
                        values.push(Some(first_value));
                        prev_value = first_value;
                    } else {
                        let value = prev_value + delta;
                        values.push(Some(value));
                        prev_value = value;
                    }
                }
            }
            
            let reconstructed = Arc::new(Float64Array::from(values)) as Arc<dyn Array>;
            let mut decompressed = fragment.clone();
            decompressed.array = Some(reconstructed);
            decompressed.metadata.compression = CompressionType::None;
            
            Ok(decompressed)
        }
        _ => Ok(fragment.clone()),
    }
}

fn decompress_bitpacked(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    if !matches!(fragment.metadata.compression, CompressionType::BitPacked) {
        return Ok(fragment.clone());
    }
    
    let array = match &fragment.array {
        Some(arr) => arr,
        None => return Ok(fragment.clone()),
    };
    
    // Get min value from metadata
    let min_val = match &fragment.metadata.min_value {
        Some(crate::storage::fragment::Value::Int64(v)) => *v as i32,
        Some(crate::storage::fragment::Value::Int32(v)) => *v,
        _ => anyhow::bail!("Bit-packed fragment missing min value"),
    };
    
    match array.data_type() {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to Int32Array"))?;
            
            // Reconstruct original values: value = min + offset
            let mut values = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(None);
                } else {
                    let offset = arr.value(i);
                    let value = min_val.wrapping_add(offset);
                    values.push(Some(value as i64)); // Convert to Int64
                }
            }
            
            let reconstructed = Arc::new(Int64Array::from(values)) as Arc<dyn Array>;
            let mut decompressed = fragment.clone();
            decompressed.array = Some(reconstructed);
            decompressed.metadata.compression = CompressionType::None;
            
            Ok(decompressed)
        }
        _ => Ok(fragment.clone()),
    }
}

fn decompress_rle(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use arrow::datatypes::DataType;
    
    if !matches!(fragment.metadata.compression, CompressionType::RLE) {
        return Ok(fragment.clone());
    }
    
    // Get RLE data from metadata
    let values_str = fragment.metadata.metadata.get("rle_values")
        .ok_or_else(|| anyhow::anyhow!("RLE fragment missing values"))?;
    let counts_str = fragment.metadata.metadata.get("rle_counts")
        .ok_or_else(|| anyhow::anyhow!("RLE fragment missing counts"))?;
    let original_len_str = fragment.metadata.metadata.get("rle_original_len")
        .ok_or_else(|| anyhow::anyhow!("RLE fragment missing original length"))?;
    
    let original_len: usize = original_len_str.parse()
        .map_err(|_| anyhow::anyhow!("Invalid original length in RLE fragment"))?;
    
    // Determine type from array
    let array = fragment.array.as_ref()
        .ok_or_else(|| anyhow::anyhow!("RLE fragment missing array"))?;
    
    match array.data_type() {
        DataType::Int64 => {
            // Parse values and counts
            let values: Vec<Option<i64>> = values_str.split(',')
                .map(|s| {
                    if s == "NULL" {
                        None
                    } else {
                        s.parse().ok()
                    }
                })
                .collect();
            
            let counts: Vec<usize> = counts_str.split(',')
                .filter_map(|s| s.parse().ok())
                .collect();
            
            if values.len() != counts.len() {
                anyhow::bail!("RLE values and counts length mismatch");
            }
            
            // Reconstruct original array
            let mut reconstructed = Vec::with_capacity(original_len);
            for (val, &count) in values.iter().zip(counts.iter()) {
                for _ in 0..count {
                    reconstructed.push(*val);
                }
            }
            
            let decompressed_array = Arc::new(Int64Array::from(reconstructed)) as Arc<dyn Array>;
            let mut decompressed = fragment.clone();
            decompressed.array = Some(decompressed_array);
            decompressed.metadata.compression = CompressionType::None;
            
            Ok(decompressed)
        }
        DataType::Utf8 => {
            // Parse string values (using | as separator)
            let values: Vec<Option<String>> = values_str.split('|')
                .map(|s| {
                    if s == "NULL" {
                        None
                    } else {
                        Some(s.to_string())
                    }
                })
                .collect();
            
            let counts: Vec<usize> = counts_str.split(',')
                .filter_map(|s| s.parse().ok())
                .collect();
            
            if values.len() != counts.len() {
                anyhow::bail!("RLE values and counts length mismatch");
            }
            
            let mut reconstructed = Vec::with_capacity(original_len);
            for (val, &count) in values.iter().zip(counts.iter()) {
                for _ in 0..count {
                    reconstructed.push(val.as_ref().map(|s| s.as_str()));
                }
            }
            
            let decompressed_array = Arc::new(StringArray::from(reconstructed)) as Arc<dyn Array>;
            let mut decompressed = fragment.clone();
            decompressed.array = Some(decompressed_array);
            decompressed.metadata.compression = CompressionType::None;
            
            Ok(decompressed)
        }
        _ => Ok(fragment.clone()),
    }
}

fn decompress_zstd(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use zstd::decode_all;
    
    // If fragment is not compressed or already decompressed, return as-is
    if !matches!(fragment.metadata.compression, CompressionType::Zstd) {
        return Ok(fragment.clone());
    }
    
    // If array already exists, it's already decompressed
    if fragment.array.is_some() {
        return Ok(fragment.clone());
    }
    
    // Get compressed bytes
    let compressed_bytes = fragment.compressed_data.as_ref()
        .ok_or_else(|| anyhow::anyhow!("Zstd compressed fragment missing compressed_data"))?;
    
    // Decompress with zstd
    let decompressed_bytes = decode_all(compressed_bytes.as_slice())?;
    
    // Deserialize array from bytes (data type is stored in serialized bytes header)
    let array = deserialize_array_simple(&decompressed_bytes)?;
    
    // Create decompressed fragment
    let mut decompressed_fragment = fragment.clone();
    decompressed_fragment.array = Some(array);
    decompressed_fragment.metadata.compression = CompressionType::None;
    decompressed_fragment.compressed_data = None;
    
    Ok(decompressed_fragment)
}

/// Simple deserialization helper for arrays (temporary - should use Arrow IPC)
fn deserialize_array_simple(bytes: &[u8]) -> Result<Arc<dyn Array>, anyhow::Error> {
    if bytes.is_empty() {
        anyhow::bail!("Cannot deserialize empty bytes");
    }
    
    // Read data type identifier (1 byte)
    let type_id = bytes[0];
    let data_bytes = &bytes[1..];
    
    // Read length (8 bytes for usize in little-endian)
    if data_bytes.len() < 8 {
        anyhow::bail!("Invalid compressed data: too short");
    }
    let len = usize::from_le_bytes([
        data_bytes[0], data_bytes[1], data_bytes[2], data_bytes[3],
        data_bytes[4], data_bytes[5], data_bytes[6], data_bytes[7],
    ]);
    
    let mut offset = 8;
    
    // Deserialize based on data type
    match type_id {
        0 => { // Int64
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                if offset >= data_bytes.len() {
                    break;
                }
                let is_null = data_bytes[offset] == 0;
                offset += 1;
                if is_null {
                    values.push(None);
                } else {
                    if offset + 8 > data_bytes.len() {
                        break;
                    }
                    let val = i64::from_le_bytes([
                        data_bytes[offset], data_bytes[offset+1], data_bytes[offset+2], data_bytes[offset+3],
                        data_bytes[offset+4], data_bytes[offset+5], data_bytes[offset+6], data_bytes[offset+7],
                    ]);
                    offset += 8;
                    values.push(Some(val));
                }
            }
            Ok(Arc::new(Int64Array::from(values)))
        }
        2 => { // Float64
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                if offset >= data_bytes.len() {
                    break;
                }
                let is_null = data_bytes[offset] == 0;
                offset += 1;
                if is_null {
                    values.push(None);
                } else {
                    if offset + 8 > data_bytes.len() {
                        break;
                    }
                    let val = f64::from_le_bytes([
                        data_bytes[offset], data_bytes[offset+1], data_bytes[offset+2], data_bytes[offset+3],
                        data_bytes[offset+4], data_bytes[offset+5], data_bytes[offset+6], data_bytes[offset+7],
                    ]);
                    offset += 8;
                    values.push(Some(val));
                }
            }
            Ok(Arc::new(Float64Array::from(values)))
        }
        4 | 5 => { // Utf8 or LargeUtf8
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                if offset >= data_bytes.len() {
                    break;
                }
                let is_null = data_bytes[offset] == 0;
                offset += 1;
                if is_null {
                    values.push(None);
                } else {
                    // Read string length (8 bytes)
                    if offset + 8 > data_bytes.len() {
                        break;
                    }
                    let str_len = usize::from_le_bytes([
                        data_bytes[offset], data_bytes[offset+1], data_bytes[offset+2], data_bytes[offset+3],
                        data_bytes[offset+4], data_bytes[offset+5], data_bytes[offset+6], data_bytes[offset+7],
                    ]);
                    offset += 8;
                    
                    // Read string bytes
                    if offset + str_len > data_bytes.len() {
                        break;
                    }
                    let str_bytes = &data_bytes[offset..offset+str_len];
                    let val = String::from_utf8(str_bytes.to_vec())
                        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in compressed data: {}", e))?;
                    offset += str_len;
                    values.push(Some(val));
                }
            }
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => {
            anyhow::bail!("Unsupported data type ID for decompression: {}", type_id);
        }
    }
}

fn decompress_lz4(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    use lz4::block::decompress;
    
    if !matches!(fragment.metadata.compression, CompressionType::Lz4) {
        return Ok(fragment.clone());
    }
    
    if fragment.array.is_some() {
        return Ok(fragment.clone()); // Already decompressed
    }
    
    let compressed_bytes = fragment.compressed_data.as_ref()
        .ok_or_else(|| anyhow::anyhow!("LZ4 compressed fragment missing compressed_data"))?;
    
    // Decompress with LZ4
    // Note: LZ4 needs to know the decompressed size, which we should store in metadata
    // For now, we'll estimate or use a reasonable buffer size
    let estimated_size = fragment.metadata.memory_size * 4; // Estimate 4× compression ratio
    let estimated_size_i32 = estimated_size.min(i32::MAX as usize) as i32;
    let decompressed_bytes = decompress(compressed_bytes, Some(estimated_size_i32))
        .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))?;
    
    // Deserialize array from bytes
    let array = deserialize_array_simple(&decompressed_bytes)?;
    
    let mut decompressed_fragment = fragment.clone();
    decompressed_fragment.array = Some(array);
    decompressed_fragment.metadata.compression = CompressionType::None;
    decompressed_fragment.compressed_data = None;
    
    Ok(decompressed_fragment)
}

