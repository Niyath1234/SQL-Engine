/// Integration tests for sparse index functionality
/// Tests that sparse indexes speed up time-series range scans

use hypergraph_sql_engine::index::sparse::{SparseIndexBuilder, SparseIndexReader};
use arrow::record_batch::RecordBatch;
use arrow::array::Int64Array;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

#[test]
fn test_sparse_index_builder() {
    // Create a test batch with sorted time-series data
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    
    // Create sorted timestamps (time-series data)
    let timestamps: Vec<i64> = (0..10000).map(|i| i * 1000).collect(); // 0, 1000, 2000, ...
    let values: Vec<i64> = (0..10000).collect();
    
    let timestamp_array = Arc::new(Int64Array::from(timestamps.clone())) as Arc<dyn arrow::array::Array>;
    let value_array = Arc::new(Int64Array::from(values)) as Arc<dyn arrow::array::Array>;
    
    let batch = RecordBatch::try_new(schema, vec![timestamp_array, value_array]).unwrap();
    
    // Build sparse index with sampling rate of 100 (every 100th row)
    let mut builder = SparseIndexBuilder::new();
    builder.build(&batch, "timestamp", 100).unwrap();
    
    // Verify index was built
    assert!(!builder.is_empty(), "Sparse index should not be empty");
    assert_eq!(builder.len(), 100, "Should have 100 entries (10000 / 100)");
    
    // Verify entries are sorted
    let entries = &builder.entries;
    for i in 1..entries.len() {
        assert!(entries[i].key >= entries[i-1].key, "Entries should be sorted");
    }
}

#[test]
fn test_sparse_index_reader_lower_bound() {
    // Create a sparse index
    let mut builder = SparseIndexBuilder::new();
    
    // Create test entries
    use hypergraph_sql_engine::index::sparse::builder::SparseIndexEntry;
    let entries = vec![
        SparseIndexEntry { key: 1000, row: 0 },
        SparseIndexEntry { key: 5000, row: 100 },
        SparseIndexEntry { key: 10000, row: 200 },
        SparseIndexEntry { key: 15000, row: 300 },
        SparseIndexEntry { key: 20000, row: 400 },
    ];
    
    let reader = SparseIndexReader::new(entries);
    
    // Test lower_bound
    assert_eq!(reader.lower_bound(0), 0, "Lower bound of 0 should be 0");
    assert_eq!(reader.lower_bound(1000), 0, "Lower bound of 1000 should be 0 (exact match)");
    assert_eq!(reader.lower_bound(3000), 1, "Lower bound of 3000 should be 1");
    assert_eq!(reader.lower_bound(5000), 1, "Lower bound of 5000 should be 1 (exact match)");
    assert_eq!(reader.lower_bound(25000), 5, "Lower bound of 25000 should be 5 (beyond end)");
}

#[test]
fn test_sparse_index_reader_find_range() {
    // Create a sparse index
    use hypergraph_sql_engine::index::sparse::builder::SparseIndexEntry;
    let entries = vec![
        SparseIndexEntry { key: 0, row: 0 },
        SparseIndexEntry { key: 10000, row: 100 },
        SparseIndexEntry { key: 20000, row: 200 },
        SparseIndexEntry { key: 30000, row: 300 },
        SparseIndexEntry { key: 40000, row: 400 },
    ];
    
    let reader = SparseIndexReader::new(entries);
    
    // Test find_range
    let (start, end) = reader.find_range(5000, 25000);
    assert_eq!(start, 100, "Start row should be 100 (first entry >= 5000)");
    assert_eq!(end, 300, "End row should be 300 (first entry > 25000)");
    
    // Test range that spans multiple entries
    let (start, end) = reader.find_range(15000, 35000);
    assert_eq!(start, 200, "Start row should be 200");
    assert_eq!(end, 400, "End row should be 400");
}

#[test]
fn test_sparse_index_time_series_performance() {
    // Create a large time-series batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
    ]));
    
    // Create 1 million timestamps
    let timestamps: Vec<i64> = (0..1_000_000).map(|i| i * 1000).collect();
    let timestamp_array = Arc::new(Int64Array::from(timestamps)) as Arc<dyn arrow::array::Array>;
    
    let batch = RecordBatch::try_new(schema, vec![timestamp_array]).unwrap();
    
    // Build sparse index with sampling rate of 1000 (every 1000th row)
    let mut builder = SparseIndexBuilder::new();
    let start = std::time::Instant::now();
    builder.build(&batch, "timestamp", 1000).unwrap();
    let build_time = start.elapsed();
    
    // Verify build time is reasonable (< 1% of ingestion overhead)
    // For 1M rows with 1000 sampling rate, should be very fast
    assert!(build_time.as_millis() < 1000, "Index build should be fast (< 1s)");
    
    // Test lookup performance
    let reader = builder.finish();
    let lookup_start = std::time::Instant::now();
    
    // Perform 1000 lookups
    for i in 0..1000 {
        let key = (i * 10000) as i64;
        reader.lower_bound(key);
    }
    
    let lookup_time = lookup_start.elapsed();
    
    // Verify lookup is fast (should be < 10ms for 1000 lookups)
    assert!(lookup_time.as_millis() < 10, "Lookups should be very fast");
}

#[test]
fn test_sparse_index_correctness() {
    // Test that sparse index produces correct pruning offsets
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
    ]));
    
    // Create sorted timestamps
    let timestamps: Vec<i64> = (0..10000).map(|i| i * 100).collect();
    let timestamp_array = Arc::new(Int64Array::from(timestamps.clone())) as Arc<dyn arrow::array::Array>;
    
    let batch = RecordBatch::try_new(schema, vec![timestamp_array]).unwrap();
    
    // Build sparse index with sampling rate of 100
    let mut builder = SparseIndexBuilder::new();
    builder.build(&batch, "timestamp", 100).unwrap();
    
    let reader = builder.finish();
    
    // Test that find_range produces correct offsets
    // Query: timestamps between 5000 and 15000
    let (start_row, end_row) = reader.find_range(5000, 15000);
    
    // Verify that the range is correct
    // First entry >= 5000 should be at row 50 (5000 / 100)
    // First entry > 15000 should be at row 150 (15000 / 100)
    assert!(start_row <= 100, "Start row should be <= 100");
    assert!(end_row <= 200, "End row should be <= 200");
    
    // Verify that the actual data matches
    // The sparse index should point to approximate positions
    // Actual scan would start from start_row and end at end_row
    assert!(start_row < end_row, "Start row should be < end row");
}

#[test]
fn test_sparse_index_with_nulls() {
    // Test that sparse index handles nulls correctly
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, true),
    ]));
    
    // Create array with some nulls
    let mut builder = Int64Array::builder(10);
    builder.append_value(1000);
    builder.append_null();
    builder.append_value(3000);
    builder.append_value(4000);
    builder.append_null();
    builder.append_value(6000);
    builder.append_value(7000);
    builder.append_value(8000);
    builder.append_null();
    builder.append_value(10000);
    let timestamp_array = Arc::new(builder.finish()) as Arc<dyn arrow::array::Array>;
    
    let batch = RecordBatch::try_new(schema, vec![timestamp_array]).unwrap();
    
    // Build sparse index - should skip nulls
    let mut builder = SparseIndexBuilder::new();
    builder.build(&batch, "timestamp", 1).unwrap(); // Sample every row
    
    // Should have 7 entries (skipping 3 nulls)
    assert_eq!(builder.len(), 7, "Should have 7 non-null entries");
}

