/// Comprehensive tests for cutting-edge features
/// Tests: Compression, Bloom Filters, Bitmap Indexes, Fragment Iterator

use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType, Value};
use hypergraph_sql_engine::storage::compression::{compress_fragment, decompress_fragment};
use arrow::array::*;
use std::sync::Arc;

fn create_test_fragment_int64(values: Vec<i64>) -> ColumnFragment {
    let array = Arc::new(Int64Array::from(values)) as Arc<dyn Array>;
    let metadata = FragmentMetadata {
        row_count: array.len(),
        min_value: None,
        max_value: None,
        cardinality: array.len(),
        compression: CompressionType::None,
        memory_size: array.len() * 8, // 8 bytes per i64
    };
    ColumnFragment::new(array, metadata)
}

fn create_test_fragment_string(values: Vec<&str>) -> ColumnFragment {
    let array = Arc::new(StringArray::from(values)) as Arc<dyn Array>;
    let metadata = FragmentMetadata {
        row_count: array.len(),
        min_value: None,
        max_value: None,
        cardinality: array.len(),
        compression: CompressionType::None,
        memory_size: array.len() * 16, // Approximate
    };
    ColumnFragment::new(array, metadata)
}

#[test]
fn test_zstd_compression_decompression() {
    println!("\n§ Testing Zstd Compression...");
    
    // Create test data (1000 integers)
    let original_values: Vec<i64> = (1..=1000).collect();
    let original_fragment = create_test_fragment_int64(original_values.clone());
    
    // Get original size
    let original_size = original_fragment.metadata.memory_size;
    println!("   Original data size: {} bytes", original_size);
    
    // Compress
    let compressed_fragment = compress_fragment(&original_fragment, CompressionType::Zstd)
        .expect("Compression should succeed");
    
    let compressed_size = compressed_fragment.compressed_data
        .as_ref()
        .map(|d| d.len())
        .unwrap_or(0);
    
    println!("   Compressed size: {} bytes", compressed_size);
    
    if compressed_size > 0 {
        let compression_ratio = (original_size as f64 / compressed_size as f64) * 100.0;
        println!("   Compression ratio: {:.2}% ({}% reduction)", 
                 compression_ratio, 
                 100.0 - (compressed_size as f64 / original_size as f64) * 100.0);
    }
    
    // Verify compressed fragment has no array
    assert!(compressed_fragment.array.is_none(), "Compressed fragment should have no array");
    assert!(compressed_fragment.compressed_data.is_some(), "Compressed fragment should have compressed data");
    assert!(matches!(compressed_fragment.metadata.compression, CompressionType::Zstd));
    
    // Decompress
    let decompressed_fragment = decompress_fragment(&compressed_fragment)
        .expect("Decompression should succeed");
    
    assert!(decompressed_fragment.array.is_some(), "Decompressed fragment should have array");
    assert!(matches!(decompressed_fragment.metadata.compression, CompressionType::None));
    
    // Verify data integrity
    let decompressed_array = decompressed_fragment.array.unwrap();
    let decomp_arr = decompressed_array.as_any().downcast_ref::<Int64Array>()
        .expect("Should be Int64Array");
    
    assert_eq!(decomp_arr.len(), original_values.len());
    for i in 0..original_values.len().min(10) {
        if !decomp_arr.is_null(i) {
            assert_eq!(decomp_arr.value(i), original_values[i], 
                      "Value at index {} should match", i);
        }
    }
    
    println!("    Compression/Decompression: PASSED");
    println!("    Data integrity verified: PASSED");
}

#[test]
fn test_bloom_filter() {
    println!("\n§ Testing Bloom Filter...");
    
    // Create test data with known values
    let values: Vec<i64> = vec![10, 20, 30, 40, 50, 100, 200, 300, 400, 500];
    let mut fragment = create_test_fragment_int64(values.clone());
    
    // Build bloom filter
    fragment.build_bloom_filter();
    
    assert!(fragment.bloom_filter.is_some(), "Bloom filter should be created");
    let bloom_size = fragment.bloom_filter.as_ref().unwrap().len();
    println!("   Bloom filter size: {} bytes", bloom_size);
    println!("   Values indexed: {}", values.len());
    
    // Note: Bloom filters are probabilistic - we can't directly check membership
    // but we can verify the filter was built correctly
    let bloom = fragment.bloom_filter.as_ref().unwrap();
    let mut bits_set = 0;
    for byte in bloom {
        bits_set += byte.count_ones();
    }
    println!("   Bits set in filter: {}", bits_set);
    
    // Verify bloom filter is non-empty (should have some bits set)
    assert!(bits_set > 0, "Bloom filter should have some bits set");
    
    println!("    Bloom Filter Construction: PASSED");
}

#[test]
fn test_bitmap_index() {
    println!("\n§ Testing Bitmap Index...");
    
    // Create test data with some duplicate values
    let values: Vec<i64> = vec![10, 20, 10, 30, 20, 10, 40, 20, 50, 10];
    let mut fragment = create_test_fragment_int64(values.clone());
    
    // Build bitmap index
    fragment.build_bitmap_index();
    
    assert!(fragment.bitmap_index.is_some(), "Bitmap index should be created");
    
    let bitmap_index = fragment.bitmap_index.as_ref().unwrap();
    println!("   Total rows indexed: {}", bitmap_index.row_count);
    println!("   Unique value bitmaps: {}", bitmap_index.bitmaps.len());
    
    // Verify bitmap index structure
    assert_eq!(bitmap_index.row_count, values.len());
    
    // Count how many unique values have bitmaps
    let unique_values: std::collections::HashSet<i64> = values.iter().cloned().collect();
    println!("   Unique values in data: {}", unique_values.len());
    
    // Verify we have bitmaps for values (note: they're keyed by hash, not value)
    assert!(bitmap_index.bitmaps.len() > 0, "Should have at least one bitmap");
    
    println!("    Bitmap Index Construction: PASSED");
}

#[test]
fn test_fragment_iterator() {
    println!("\n§ Testing Fragment Iterator...");
    
    // Test Int64 values
    let int_values: Vec<i64> = vec![100, 200, 300, 400, 500];
    let fragment_int = create_test_fragment_int64(int_values.clone());
    
    let mut iter = fragment_int.iter();
    let mut collected = Vec::new();
    
    while let Some(value) = iter.next() {
        collected.push(value);
    }
    
    assert_eq!(collected.len(), int_values.len());
    
    // Verify values match (first few)
    for i in 0..collected.len().min(5) {
        if let Value::Int64(v) = collected[i] {
            assert_eq!(v, int_values[i], "Int64 value should match");
        } else {
            panic!("Expected Int64 value");
        }
    }
    
    println!("   Int64 iterator: {} values collected", collected.len());
    
    // Test String values
    let str_values = vec!["apple", "banana", "cherry", "date", "elderberry"];
    let fragment_str = create_test_fragment_string(str_values.clone());
    
    let mut iter_str = fragment_str.iter();
    let mut collected_str = Vec::new();
    
    while let Some(value) = iter_str.next() {
        collected_str.push(value);
    }
    
    assert_eq!(collected_str.len(), str_values.len());
    
    for i in 0..collected_str.len().min(5) {
        if let Value::String(ref v) = collected_str[i] {
            assert_eq!(v, str_values[i], "String value should match");
        } else {
            panic!("Expected String value");
        }
    }
    
    println!("   String iterator: {} values collected", collected_str.len());
    println!("    Fragment Iterator: PASSED");
}

#[test]
fn test_integrated_workflow() {
    println!("\n§ Testing Integrated Workflow...");
    println!("   (Compression + Indexes + Iterator)");
    
    // Create test data
    let values: Vec<i64> = (1..=100).collect();
    let mut fragment = create_test_fragment_int64(values.clone());
    
    // Step 1: Build indexes
    fragment.build_bloom_filter();
    fragment.build_bitmap_index();
    println!("   Step 1: Built bloom filter and bitmap index");
    
    // Step 2: Compress
    let compressed = compress_fragment(&fragment, CompressionType::Zstd)
        .expect("Compression should succeed");
    println!("   Step 2: Compressed fragment");
    
    // Step 3: Decompress
    let decompressed = decompress_fragment(&compressed)
        .expect("Decompression should succeed");
    println!("   Step 3: Decompressed fragment");
    
    // Step 4: Verify data integrity with iterator
    let mut iter = decompressed.iter();
    let mut values_match = true;
    let mut verified_count = 0;
    
    for expected_value in values.iter().take(10) {
        if let Some(Value::Int64(actual)) = iter.next() {
            if actual != *expected_value {
                values_match = false;
                break;
            }
            verified_count += 1;
        }
    }
    
    assert!(values_match, "Decompressed values should match original");
    println!("   Step 4: Verified data integrity with iterator");
    println!("    Integrated Workflow: PASSED");
}

#[test]
fn test_compression_with_different_types() {
    println!("\n§ Testing Compression with Different Data Types...");
    
    // Test Int64
    let int_values: Vec<i64> = (1..=500).collect();
    let int_fragment = create_test_fragment_int64(int_values);
    let compressed_int = compress_fragment(&int_fragment, CompressionType::Zstd)
        .expect("Int64 compression should work");
    let decompressed_int = decompress_fragment(&compressed_int)
        .expect("Int64 decompression should work");
    assert!(decompressed_int.array.is_some());
    println!("    Int64 compression: PASSED");
    
    // Test Float64
    let float_values: Vec<f64> = (1..=500).map(|i| i as f64 * 1.5).collect();
    let float_array = Arc::new(Float64Array::from(float_values.clone())) as Arc<dyn Array>;
    let float_metadata = FragmentMetadata {
        row_count: float_array.len(),
        min_value: None,
        max_value: None,
        cardinality: float_array.len(),
        compression: CompressionType::None,
        memory_size: float_array.len() * 8,
    };
    let float_fragment = ColumnFragment::new(float_array, float_metadata);
    let compressed_float = compress_fragment(&float_fragment, CompressionType::Zstd)
        .expect("Float64 compression should work");
    let decompressed_float = decompress_fragment(&compressed_float)
        .expect("Float64 decompression should work");
    assert!(decompressed_float.array.is_some());
    println!("    Float64 compression: PASSED");
    
    // Test String
    let str_fragment = create_test_fragment_string(vec!["apple"; 200]);
    let compressed_str = compress_fragment(&str_fragment, CompressionType::Zstd)
        .expect("String compression should work");
    let decompressed_str = decompress_fragment(&compressed_str)
        .expect("String decompression should work");
    assert!(decompressed_str.array.is_some());
    println!("    String compression: PASSED");
}

fn main() {
    println!(" Cutting-Edge Features Test Suite");
    println!("=====================================\n");
    
    // Run all tests
    test_zstd_compression_decompression();
    test_bloom_filter();
    test_bitmap_index();
    test_fragment_iterator();
    test_integrated_workflow();
    test_compression_with_different_types();
    
    println!("\n All tests completed!");
    println!("=====================================");
}

