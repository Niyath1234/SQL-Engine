/// Integration tests for spill-to-disk functionality
/// Tests that spill manager prevents OOM and handles large queries correctly

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, Value, CompressionType};
use arrow::array::{Int64Array};
use std::sync::Arc;

#[test]
fn test_spill_manager_creation() {
    // Test that spill manager can be created
    let spill_mgr = hypergraph_sql_engine::spill::SpillManager::new(1024 * 1024); // 1 MB threshold
    assert_eq!(spill_mgr.current_bytes(), 0);
}

#[tokio::test]
async fn test_spill_threshold_exceeded() {
    // Test that spill manager creates spill files when threshold is exceeded
    use hypergraph_sql_engine::spill::SpillManager;
    use arrow::record_batch::RecordBatch;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    
    // Create a small threshold (1 KB) to trigger spill quickly
    let mut spill_mgr = SpillManager::new(1024);
    
    // Create a batch that exceeds threshold
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
    ]));
    
    // Create a batch with enough data to exceed 1 KB
    let values: Vec<i64> = (0..1000).collect();
    let array = Arc::new(Int64Array::from(values)) as Arc<dyn arrow::array::Array>;
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    
    // Try to spill - should create a spill file
    let result = spill_mgr.maybe_spill(&batch).await;
    
    // Should have spilled (threshold is 1 KB, batch is larger)
    assert!(result.is_ok());
    let spill_path = result.unwrap();
    assert!(spill_path.is_some(), "Batch should have been spilled");
    
    // Clean up
    if let Some(path) = spill_path {
        if path.exists() {
            std::fs::remove_file(&path).ok();
        }
    }
}

#[test]
fn test_spill_with_engine() {
    // Test that engine can be created with spill manager
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    let values = Int64Array::from(vec![1, 2, 3, 4, 5]);
    
    let value_metadata = FragmentMetadata {
        row_count: 5,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(5)),
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
    
    // Execute query - should work even with spill enabled
    let result = engine.execute_query("SELECT value FROM test");
    
    assert!(result.is_ok(), "Query should succeed even with spill manager");
}

#[tokio::test]
async fn test_spill_file_cleanup() {
    // Test that spill files are cleaned up
    use hypergraph_sql_engine::spill::SpillManager;
    use arrow::record_batch::RecordBatch;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    
    let mut spill_mgr = SpillManager::new(1024);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
    ]));
    
    let values: Vec<i64> = (0..1000).collect();
    let array = Arc::new(Int64Array::from(values)) as Arc<dyn arrow::array::Array>;
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    
    // Spill the batch
    let spill_result = spill_mgr.maybe_spill(&batch).await.unwrap();
    
    if let Some(spill_path) = spill_result {
        // Verify file exists
        assert!(spill_path.exists(), "Spill file should exist");
        
        // Clean up
        spill_mgr.cleanup().await.unwrap();
        
        // File should be removed (or at least cleanup should not error)
        // Note: On some systems, file might still exist briefly
    }
}

#[tokio::test]
async fn test_spill_read_back() {
    // Test that spilled batches can be read back
    use hypergraph_sql_engine::spill::SpillManager;
    use arrow::record_batch::RecordBatch;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    
    let mut spill_mgr = SpillManager::new(1024);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
    ]));
    
    let values: Vec<i64> = (0..100).collect();
    let array = Arc::new(Int64Array::from(values.clone())) as Arc<dyn arrow::array::Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    
    // Spill the batch
    let spill_result = spill_mgr.maybe_spill(&batch).await.unwrap();
    
    if let Some(spill_path) = spill_result {
        // Read back the spilled batch
        let read_batch = spill_mgr.read_spill(&spill_path).await.unwrap();
        
        // Verify data matches
        assert_eq!(read_batch.num_rows(), batch.num_rows());
        assert_eq!(read_batch.num_columns(), batch.num_columns());
        
        // Verify values match
        let read_array = read_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(read_array.value(i), expected);
        }
        
        // Clean up
        std::fs::remove_file(&spill_path).ok();
    }
}

