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
fn compress_delta(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement delta encoding
    // Store differences between consecutive values
    Ok(fragment.clone())
}

/// Bit-packing for integers
fn compress_bitpacked(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement bit-packing
    // Pack integers into minimal bits needed
    Ok(fragment.clone())
}

/// Run-length encoding for repeated values
fn compress_rle(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement RLE
    // Store (value, count) pairs
    Ok(fragment.clone())
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
fn compress_lz4(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement LZ4 compression
    Ok(fragment.clone())
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
    // TODO: Implement dictionary decompression
    Ok(fragment.clone())
}

fn decompress_delta(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement delta decompression
    Ok(fragment.clone())
}

fn decompress_bitpacked(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement bitpacked decompression
    Ok(fragment.clone())
}

fn decompress_rle(fragment: &ColumnFragment) -> Result<ColumnFragment, anyhow::Error> {
    // TODO: Implement RLE decompression
    Ok(fragment.clone())
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
    // TODO: Implement LZ4 decompression
    Ok(fragment.clone())
}

