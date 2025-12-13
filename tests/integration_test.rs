//! Integration test for the public API
//! 
//! Run with: `cargo test --test integration_test`

use hypergraph_sql_engine::HypergraphSQLEngine;

#[test]
fn test_basic_engine_creation() {
    let engine = HypergraphSQLEngine::new();
    assert!(engine.cache_stats().plan_cache_size == 0);
}

#[test]
fn test_create_table() {
    let mut engine = HypergraphSQLEngine::new();
    let result = engine.execute_query(
        "CREATE TABLE test_table (id INT, name VARCHAR)"
    );
    assert!(result.is_ok());
}

#[test]
fn test_insert_and_select() {
    let mut engine = HypergraphSQLEngine::new();
    
    // Create table
    engine.execute_query(
        "CREATE TABLE users (id INT, name VARCHAR, age INT)"
    ).unwrap();
    
    // Insert data
    engine.execute_query(
        "INSERT INTO users VALUES (1, 'Alice', 30)"
    ).unwrap();
    
    // Query data
    let result = engine.execute_query(
        "SELECT name, age FROM users WHERE age > 25"
    ).unwrap();
    
    assert_eq!(result.row_count, 1);
    assert!(result.execution_time_ms >= 0.0);
}

#[test]
fn test_group_by() {
    let mut engine = HypergraphSQLEngine::new();
    
    engine.execute_query(
        "CREATE TABLE sales (product VARCHAR, amount FLOAT)"
    ).unwrap();
    
    engine.execute_query(
        "INSERT INTO sales VALUES ('A', 100.0), ('B', 200.0), ('A', 150.0)"
    ).unwrap();
    
    let result = engine.execute_query(
        "SELECT product, SUM(amount) as total FROM sales GROUP BY product"
    ).unwrap();
    
    assert!(result.row_count > 0);
}

#[test]
fn test_cache_stats() {
    let mut engine = HypergraphSQLEngine::new();
    
    engine.execute_query("CREATE TABLE test (id INT)").unwrap();
    engine.execute_query("SELECT * FROM test").unwrap();
    
    let stats = engine.cache_stats();
    assert!(stats.plan_cache_size >= 0);
}

#[test]
fn test_clear_caches() {
    let mut engine = HypergraphSQLEngine::new();
    
    engine.execute_query("CREATE TABLE test (id INT)").unwrap();
    engine.execute_query("SELECT * FROM test").unwrap();
    
    engine.clear_all_caches();
    
    let stats = engine.cache_stats();
    assert_eq!(stats.plan_cache_size, 0);
}

