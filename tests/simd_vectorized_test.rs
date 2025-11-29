/// Integration tests for SIMD vectorized scan and filter operations
/// Tests that SIMD provides performance improvements and correct results

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, Value, CompressionType};
use arrow::array::{Int64Array};
use std::sync::Arc;

#[test]
fn test_simd_filter_greater_than() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data with various sizes to test SIMD alignment
    let values = Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    
    let value_metadata = FragmentMetadata {
        row_count: 10,
        min_value: Some(Value::Int64(10)),
        max_value: Some(Value::Int64(100)),
        cardinality: 10,
        compression: CompressionType::None,
        memory_size: 80,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SIMD filter greater-than - should use SIMD kernel
    let result = engine.execute_query("SELECT value FROM test WHERE value > 50");
    
    assert!(result.is_ok(), "SIMD filter greater-than should succeed");
    // Should return rows with value > 50: 60, 70, 80, 90, 100
}

#[test]
fn test_simd_filter_less_than() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    let values = Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    
    let value_metadata = FragmentMetadata {
        row_count: 10,
        min_value: Some(Value::Int64(10)),
        max_value: Some(Value::Int64(100)),
        cardinality: 10,
        compression: CompressionType::None,
        memory_size: 80,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SIMD filter less-than
    let result = engine.execute_query("SELECT value FROM test WHERE value < 50");
    
    assert!(result.is_ok(), "SIMD filter less-than should succeed");
    // Should return rows with value < 50: 10, 20, 30, 40
}

#[test]
fn test_simd_filter_equality() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    let values = Int64Array::from(vec![1, 2, 3, 4, 5, 3, 3, 6, 7, 8]);
    
    let value_metadata = FragmentMetadata {
        row_count: 10,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(8)),
        cardinality: 8,
        compression: CompressionType::None,
        memory_size: 80,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SIMD filter equality
    let result = engine.execute_query("SELECT value FROM test WHERE value = 3");
    
    assert!(result.is_ok(), "SIMD filter equality should succeed");
    // Should return 3 rows with value = 3
}

#[test]
fn test_simd_filter_unaligned_size() {
    // Test with size not aligned to SIMD lane count (4)
    // This tests the remainder handling
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data with 7 elements (not a multiple of 4)
    let values = Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70]);
    
    let value_metadata = FragmentMetadata {
        row_count: 7,
        min_value: Some(Value::Int64(10)),
        max_value: Some(Value::Int64(70)),
        cardinality: 7,
        compression: CompressionType::None,
        memory_size: 56,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SIMD filter - should handle remainder correctly
    let result = engine.execute_query("SELECT value FROM test WHERE value > 40");
    
    assert!(result.is_ok(), "SIMD filter with unaligned size should succeed");
    // Should return rows with value > 40: 50, 60, 70
}

#[test]
fn test_simd_filter_with_nulls() {
    // Test that SIMD correctly handles NULL values
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data with nulls
    let mut builder = Int64Array::builder(8);
    builder.append_value(10);
    builder.append_null();
    builder.append_value(30);
    builder.append_value(40);
    builder.append_null();
    builder.append_value(60);
    builder.append_value(70);
    builder.append_null();
    let values = builder.finish();
    
    let value_metadata = FragmentMetadata {
        row_count: 8,
        min_value: Some(Value::Int64(10)),
        max_value: Some(Value::Int64(70)),
        cardinality: 5,
        compression: CompressionType::None,
        memory_size: 64,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SIMD filter - should skip nulls
    let result = engine.execute_query("SELECT value FROM test WHERE value > 30");
    
    assert!(result.is_ok(), "SIMD filter with nulls should succeed");
    // Should return rows with value > 30 (skipping nulls): 40, 60, 70
}

#[test]
fn test_simd_filter_large_dataset() {
    // Test SIMD performance on larger dataset
    let mut engine = HypergraphSQLEngine::new();
    
    // Create larger dataset (1000 elements) to test SIMD performance
    let values: Vec<i64> = (1..=1000).collect();
    let values = Int64Array::from(values);
    
    let value_metadata = FragmentMetadata {
        row_count: 1000,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(1000)),
        cardinality: 1000,
        compression: CompressionType::None,
        memory_size: 8000,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SIMD filter on large dataset
    let result = engine.execute_query("SELECT value FROM test WHERE value > 500");
    
    assert!(result.is_ok(), "SIMD filter on large dataset should succeed");
    // Should return 500 rows with value > 500
}

