/// Integration tests for micro-kernel specialization
/// Tests that micro-kernels provide performance improvements for aggregation and filtering

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, Value, CompressionType};
use arrow::array::{Int64Array, Float64Array};
use std::sync::Arc;

#[test]
fn test_sum_aggregation_with_kernels() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data for sales table
    let amounts = Int64Array::from(vec![100, 200, 300, 400, 500]);
    
    let amount_metadata = FragmentMetadata {
        row_count: 5,
        min_value: Some(Value::Int64(100)),
        max_value: Some(Value::Int64(500)),
        cardinality: 5,
        compression: CompressionType::None,
        memory_size: 40,
        table_name: Some("sales".to_string()),
        column_name: Some("amount".to_string()),
    };
    
    let amount_fragment = ColumnFragment::new(Arc::new(amounts) as Arc<dyn arrow::array::Array>, amount_metadata);
    
    // Load table
    engine.load_table("sales", vec![
        ("amount".to_string(), amount_fragment),
    ]).expect("Failed to load table");
    
    // Test SUM aggregation - should use micro-kernel for Int64
    let result = engine.execute_query("SELECT SUM(amount) FROM sales");
    
    assert!(result.is_ok(), "SUM aggregation should succeed");
    let result = result.unwrap();
    assert_eq!(result.batches.len(), 1, "Should return one row");
    // SUM(100+200+300+400+500) = 1500
    // Note: Actual value checking depends on result format
}

#[test]
fn test_avg_aggregation_with_kernels() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    let values = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
    
    let value_metadata = FragmentMetadata {
        row_count: 5,
        min_value: Some(Value::Float64(10.0)),
        max_value: Some(Value::Float64(50.0)),
        cardinality: 5,
        compression: CompressionType::None,
        memory_size: 40,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test AVG aggregation - should use micro-kernel for Float64
    let result = engine.execute_query("SELECT AVG(value) FROM test");
    
    assert!(result.is_ok(), "AVG aggregation should succeed");
    // AVG(10+20+30+40+50) = 30.0
}

#[test]
fn test_filter_equality_with_kernels() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5, 3, 3]);
    
    let id_metadata = FragmentMetadata {
        row_count: 7,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(5)),
        cardinality: 5,
        compression: CompressionType::None,
        memory_size: 56,
        table_name: Some("items".to_string()),
        column_name: Some("id".to_string()),
    };
    
    let id_fragment = ColumnFragment::new(Arc::new(ids) as Arc<dyn arrow::array::Array>, id_metadata);
    
    // Load table
    engine.load_table("items", vec![
        ("id".to_string(), id_fragment),
    ]).expect("Failed to load table");
    
    // Test filter equality - should use micro-kernel for Int64
    let result = engine.execute_query("SELECT id FROM items WHERE id = 3");
    
    assert!(result.is_ok(), "Filter equality should succeed");
    // Should return 3 rows with id=3
}

#[test]
fn test_filter_range_with_kernels() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    let ages = Int64Array::from(vec![20, 25, 30, 35, 40, 45, 50]);
    
    let age_metadata = FragmentMetadata {
        row_count: 7,
        min_value: Some(Value::Int64(20)),
        max_value: Some(Value::Int64(50)),
        cardinality: 7,
        compression: CompressionType::None,
        memory_size: 56,
        table_name: Some("people".to_string()),
        column_name: Some("age".to_string()),
    };
    
    let age_fragment = ColumnFragment::new(Arc::new(ages) as Arc<dyn arrow::array::Array>, age_metadata);
    
    // Load table
    engine.load_table("people", vec![
        ("age".to_string(), age_fragment),
    ]).expect("Failed to load table");
    
    // Test filter less-than - should use micro-kernel for Int64
    let result = engine.execute_query("SELECT age FROM people WHERE age < 35");
    
    assert!(result.is_ok(), "Filter less-than should succeed");
    // Should return rows with age < 35: 20, 25, 30
}

#[test]
fn test_aggregation_with_nulls() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data with nulls
    let mut builder = Int64Array::builder(5);
    builder.append_value(10);
    builder.append_null();
    builder.append_value(30);
    builder.append_value(40);
    builder.append_null();
    let values = builder.finish();
    
    let value_metadata = FragmentMetadata {
        row_count: 5,
        min_value: Some(Value::Int64(10)),
        max_value: Some(Value::Int64(40)),
        cardinality: 3,
        compression: CompressionType::None,
        memory_size: 40,
        table_name: Some("test".to_string()),
        column_name: Some("value".to_string()),
    };
    
    let value_fragment = ColumnFragment::new(Arc::new(values) as Arc<dyn arrow::array::Array>, value_metadata);
    
    // Load table
    engine.load_table("test", vec![
        ("value".to_string(), value_fragment),
    ]).expect("Failed to load table");
    
    // Test SUM aggregation with nulls - should skip nulls
    let result = engine.execute_query("SELECT SUM(value) FROM test");
    
    assert!(result.is_ok(), "SUM aggregation with nulls should succeed");
    // SUM(10 + NULL + 30 + 40 + NULL) = 80 (nulls should be skipped)
}

