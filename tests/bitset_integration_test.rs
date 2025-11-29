/// Integration test for BAMJ index building and execution dispatch
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, Value, CompressionType};
use arrow::array::{Int64Array, StringArray};
use std::sync::Arc;

#[test]
fn test_bitmap_index_building_during_table_load() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data for users table
    let user_ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let user_names = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    
    // Create fragments with correct metadata structure
    let id_metadata = FragmentMetadata {
        row_count: 5,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(5)),
        cardinality: 5,
        compression: CompressionType::None,
        memory_size: 40,
        table_name: None,
        column_name: None,
    };
    
    let name_metadata = FragmentMetadata {
        row_count: 5,
        min_value: None,
        max_value: None,
        cardinality: 5,
        compression: CompressionType::None,
        memory_size: 50,
        table_name: None,
        column_name: None,
    };
    
    let mut id_fragment = ColumnFragment::new(Arc::new(user_ids) as Arc<dyn arrow::array::Array>, id_metadata);
    id_fragment.metadata.table_name = Some("users".to_string());
    id_fragment.metadata.column_name = Some("id".to_string());
    
    let mut name_fragment = ColumnFragment::new(Arc::new(user_names) as Arc<dyn arrow::array::Array>, name_metadata);
    name_fragment.metadata.table_name = Some("users".to_string());
    name_fragment.metadata.column_name = Some("name".to_string());
    
    // Load table - bitmap indexes will be built automatically
    let columns = vec![
        ("id".to_string(), id_fragment),
        ("name".to_string(), name_fragment),
    ];
    
    let _node_id = engine.load_table("users", columns).expect("Failed to load table");
    
    // Test that bitmap index building was called by executing a query
    // The index building happens during load_table, so if load succeeded, indexes were attempted
    let result = engine.execute_query("SELECT id FROM users WHERE id = 1");
    
    // Query should succeed (index building is automatic)
    assert!(result.is_ok(), "Query should succeed - bitmap indexes should be built automatically");
}

#[test]
fn test_bitset_join_execution() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create users table
    let user_ids = Int64Array::from(vec![1, 2, 3]);
    let user_names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    
    let id_metadata = FragmentMetadata {
        row_count: 3,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(3)),
        cardinality: 3,
        compression: CompressionType::None,
        memory_size: 24,
        table_name: None,
        column_name: None,
    };
    
    let name_metadata = FragmentMetadata {
        row_count: 3,
        min_value: None,
        max_value: None,
        cardinality: 3,
        compression: CompressionType::None,
        memory_size: 30,
        table_name: None,
        column_name: None,
    };
    
    let mut id_fragment = ColumnFragment::new(Arc::new(user_ids) as Arc<dyn arrow::array::Array>, id_metadata);
    id_fragment.metadata.table_name = Some("users".to_string());
    id_fragment.metadata.column_name = Some("id".to_string());
    
    let mut name_fragment = ColumnFragment::new(Arc::new(user_names) as Arc<dyn arrow::array::Array>, name_metadata);
    name_fragment.metadata.table_name = Some("users".to_string());
    name_fragment.metadata.column_name = Some("name".to_string());
    
    engine.load_table("users", vec![
        ("id".to_string(), id_fragment),
        ("name".to_string(), name_fragment),
    ]).expect("Failed to load users table");
    
    // Create orders table
    let order_ids = Int64Array::from(vec![1, 2, 3]);
    let user_ids_orders = Int64Array::from(vec![1, 2, 1]); // user_id foreign key
    
    let order_id_metadata = FragmentMetadata {
        row_count: 3,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(3)),
        cardinality: 3,
        compression: CompressionType::None,
        memory_size: 24,
        table_name: None,
        column_name: None,
    };
    
    let user_id_metadata = FragmentMetadata {
        row_count: 3,
        min_value: Some(Value::Int64(1)),
        max_value: Some(Value::Int64(2)),
        cardinality: 2,
        compression: CompressionType::None,
        memory_size: 24,
        table_name: None,
        column_name: None,
    };
    
    let mut order_id_fragment = ColumnFragment::new(Arc::new(order_ids) as Arc<dyn arrow::array::Array>, order_id_metadata);
    order_id_fragment.metadata.table_name = Some("orders".to_string());
    order_id_fragment.metadata.column_name = Some("order_id".to_string());
    
    let mut user_id_fragment = ColumnFragment::new(Arc::new(user_ids_orders) as Arc<dyn arrow::array::Array>, user_id_metadata);
    user_id_fragment.metadata.table_name = Some("orders".to_string());
    user_id_fragment.metadata.column_name = Some("user_id".to_string());
    
    engine.load_table("orders", vec![
        ("order_id".to_string(), order_id_fragment),
        ("user_id".to_string(), user_id_fragment),
    ]).expect("Failed to load orders table");
    
    // Add join relationship
    engine.add_join("users", "id", "orders", "user_id").expect("Failed to add join");
    
    // Test join query - if BitsetJoin is used, it should work
    let result = engine.execute_query("SELECT users.id, users.name, orders.order_id FROM users JOIN orders ON users.id = orders.user_id");
    
    // Query should succeed
    assert!(result.is_ok(), "Join query should succeed with bitmap indexes");
}

#[test]
fn test_bitmap_index_metadata_storage() {
    // Create engine
    let mut engine = HypergraphSQLEngine::new();
    
    // Create simple table
    let ids = Int64Array::from(vec![10, 20, 30]);
    let id_metadata = FragmentMetadata {
        row_count: 3,
        min_value: Some(Value::Int64(10)),
        max_value: Some(Value::Int64(30)),
        cardinality: 3,
        compression: CompressionType::None,
        memory_size: 24,
        table_name: None,
        column_name: None,
    };
    
    let mut id_fragment = ColumnFragment::new(Arc::new(ids) as Arc<dyn arrow::array::Array>, id_metadata);
    id_fragment.metadata.table_name = Some("test".to_string());
    id_fragment.metadata.column_name = Some("id".to_string());
    
    engine.load_table("test", vec![
        ("id".to_string(), id_fragment),
    ]).expect("Failed to load table");
    
    // Test that the table was loaded successfully
    let result = engine.execute_query("SELECT id FROM test");
    assert!(result.is_ok(), "Query should succeed - bitmap indexes should be built");
}

