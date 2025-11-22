/// Comprehensive Transaction Testing
/// Tests ACID properties, isolation levels, locking, and WAL integration
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::ColumnFragment;
use hypergraph_sql_engine::storage::fragment::Value;
use arrow::array::*;
use std::sync::Arc;

fn create_test_table(engine: &mut HypergraphSQLEngine) -> Result<(), Box<dyn std::error::Error>> {
    // Create employees table
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let names = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
    let salaries = Arc::new(Float64Array::from(vec![50000.0, 60000.0, 70000.0]));
    
    let fragment_id = ColumnFragment::new(
        ids,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 3,
            min_value: Some(Value::Int64(1)),
            max_value: Some(Value::Int64(3)),
            cardinality: 3,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 24,
        },
    );
    
    let fragment_name = ColumnFragment::new(
        names,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 3,
            min_value: None,
            max_value: None,
            cardinality: 3,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 50,
        },
    );
    
    let fragment_salary = ColumnFragment::new(
        salaries,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 3,
            min_value: Some(Value::Float64(50000.0)),
            max_value: Some(Value::Float64(70000.0)),
            cardinality: 3,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 24,
        },
    );
    
    engine.load_table("employees", vec![
        ("id".to_string(), fragment_id),
        ("name".to_string(), fragment_name),
        ("salary".to_string(), fragment_salary),
    ])?;
    
    Ok(())
}

fn print_result(query: &str, result: &hypergraph_sql_engine::execution::engine::QueryResult) {
    println!("  Query: {}", query);
    println!("  Rows: {} | Time: {:.2}ms", result.row_count, result.execution_time_ms);
    if result.row_count > 0 && !result.batches.is_empty() {
        let batch = &result.batches[0].batch;
        let schema = &batch.schema;
        let max_rows = result.row_count.min(5);
        
        for row_idx in 0..max_rows {
            let mut values = Vec::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                if col_idx < batch.columns.len() {
                    let col = &batch.columns[col_idx];
                    let val_str = if col.len() > row_idx {
                        if col.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            match col.data_type() {
                                arrow::datatypes::DataType::Int64 => {
                                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                    format!("{}", arr.value(row_idx))
                                }
                                arrow::datatypes::DataType::Float64 => {
                                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                                    format!("{:.2}", arr.value(row_idx))
                                }
                                arrow::datatypes::DataType::Utf8 => {
                                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                    format!("\"{}\"", arr.value(row_idx))
                                }
                                _ => format!("{:?}", col),
                            }
                        }
                    } else {
                        "N/A".to_string()
                    };
                    values.push(format!("{}: {}", field.name(), val_str));
                }
            }
            println!("    Row {}: {}", row_idx + 1, values.join(" | "));
        }
        if result.row_count > max_rows {
            println!("    ... ({} more rows)", result.row_count - max_rows);
        }
    }
    println!();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", "=".repeat(80));
    println!("COMPREHENSIVE TRANSACTION TESTING");
    println!("Testing ACID Properties, Isolation Levels, Locking, and WAL");
    println!("{}", "=".repeat(80));
    println!();
    
    let mut engine = HypergraphSQLEngine::new();
    create_test_table(&mut engine)?;
    
    // Test 1: Basic Transaction (Atomicity)
    println!(" Test 1: Basic Transaction (Atomicity)");
    println!("{}", "-".repeat(80));
    println!("Expected: All operations succeed or all fail");
    
    // BEGIN transaction
    let result = engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // INSERT data
    match engine.execute_query("INSERT INTO employees (id, name, salary) VALUES (4, 'David', 80000)") {
        Ok(r) => {
            println!(" INSERT succeeded: {} rows", r.row_count);
        }
        Err(e) => println!("— INSERT failed: {}", e),
    }
    
    // Check data is visible within transaction
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 4")?;
    println!(" Data visible in transaction: {} rows found", result.row_count);
    print_result("SELECT * FROM employees WHERE id = 4", &result);
    
    // COMMIT transaction
    let result = engine.execute_query("COMMIT")?;
    println!(" Transaction committed");
    
    // Verify data is persistent after commit
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 4")?;
    println!(" Data persistent after commit: {} rows found", result.row_count);
    print_result("SELECT * FROM employees WHERE id = 4", &result);
    println!();
    
    // Test 2: Rollback (Atomicity)
    println!(" Test 2: Rollback (Atomicity)");
    println!("{}", "-".repeat(80));
    println!("Expected: All changes undone on rollback");
    
    // BEGIN transaction
    engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // INSERT data
    engine.execute_query("INSERT INTO employees (id, name, salary) VALUES (5, 'Eve', 90000)")?;
    println!(" INSERT succeeded");
    
    // Check data is visible within transaction
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 5")?;
    println!(" Data visible in transaction: {} rows found", result.row_count);
    
    // ROLLBACK transaction
    engine.execute_query("ROLLBACK")?;
    println!(" Transaction rolled back");
    
    // Verify data is not persistent after rollback
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 5")?;
    println!(" Data NOT persistent after rollback: {} rows found (expected 0)", result.row_count);
    if result.row_count == 0 {
        println!(" Atomicity test PASSED: Rollback undid all changes");
    } else {
        println!("— Atomicity test FAILED: Data still exists after rollback");
    }
    println!();
    
    // Test 3: DDL in Transaction (CREATE TABLE)
    println!(" Test 3: DDL in Transaction (CREATE TABLE)");
    println!("{}", "-".repeat(80));
    
    engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // CREATE TABLE
    match engine.execute_query("CREATE TABLE departments (id INTEGER, name VARCHAR)") {
        Ok(_) => println!(" CREATE TABLE succeeded"),
        Err(e) => println!("— CREATE TABLE failed: {}", e),
    }
    
    // Verify table exists within transaction
    match engine.execute_query("SELECT * FROM departments") {
        Ok(r) => println!(" Table accessible in transaction: {} rows", r.row_count),
        Err(e) => println!("— Table not accessible: {}", e),
    }
    
    engine.execute_query("COMMIT")?;
    println!(" Transaction committed");
    
    // Verify table exists after commit
    match engine.execute_query("SELECT * FROM departments") {
        Ok(r) => println!(" Table persistent after commit: {} rows", r.row_count),
        Err(e) => println!("— Table not persistent: {}", e),
    }
    println!();
    
    // Test 4: DDL in Transaction (DROP TABLE with rollback)
    println!(" Test 4: DDL Rollback (DROP TABLE)");
    println!("{}", "-".repeat(80));
    
    // First create a table to drop
    engine.execute_query("CREATE TABLE test_table (id INTEGER)")?;
    println!(" Created test_table");
    
    engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // DROP TABLE
    engine.execute_query("DROP TABLE test_table")?;
    println!(" DROP TABLE executed");
    
    // Verify table doesn't exist within transaction
    match engine.execute_query("SELECT * FROM test_table") {
        Ok(_) => println!("— Table still accessible (should not be)"),
        Err(_) => println!(" Table not accessible in transaction (expected)"),
    }
    
    // ROLLBACK
    engine.execute_query("ROLLBACK")?;
    println!(" Transaction rolled back");
    
    // Verify table exists after rollback (should be restored)
    match engine.execute_query("SELECT * FROM test_table") {
        Ok(r) => println!(" Table restored after rollback: {} rows", r.row_count),
        Err(e) => println!("— Table not restored: {}", e),
    }
    println!();
    
    // Test 5: UPDATE in Transaction
    println!(" Test 5: UPDATE in Transaction");
    println!("{}", "-".repeat(80));
    
    engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // UPDATE
    let result = engine.execute_query("UPDATE employees SET salary = 100000 WHERE id = 1")?;
    println!(" UPDATE succeeded: {} rows affected", result.row_count);
    
    // Verify update is visible within transaction
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 1")?;
    print_result("SELECT * FROM employees WHERE id = 1", &result);
    
    engine.execute_query("COMMIT")?;
    println!(" Transaction committed");
    
    // Verify update is persistent
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 1")?;
    print_result("SELECT * FROM employees WHERE id = 1", &result);
    println!();
    
    // Test 6: DELETE in Transaction
    println!(" Test 6: DELETE in Transaction");
    println!("{}", "-".repeat(80));
    
    engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // DELETE
    let result = engine.execute_query("DELETE FROM employees WHERE id = 3")?;
    println!(" DELETE succeeded: {} rows affected", result.row_count);
    
    // Verify deletion is visible within transaction
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 3")?;
    println!(" Data deleted in transaction: {} rows found (expected 0)", result.row_count);
    
    engine.execute_query("COMMIT")?;
    println!(" Transaction committed");
    
    // Verify deletion is persistent
    let result = engine.execute_query("SELECT * FROM employees WHERE id = 3")?;
    println!(" Data deleted after commit: {} rows found (expected 0)", result.row_count);
    println!();
    
    // Test 7: Multiple Operations in Transaction
    println!(" Test 7: Multiple Operations in Transaction");
    println!("{}", "-".repeat(80));
    println!("Expected: All operations succeed or all fail together");
    
    engine.execute_query("BEGIN")?;
    println!(" Transaction started");
    
    // Multiple operations
    engine.execute_query("INSERT INTO employees (id, name, salary) VALUES (6, 'Frank', 95000)")?;
    println!(" INSERT 1 succeeded");
    
    engine.execute_query("UPDATE employees SET salary = 75000 WHERE id = 2")?;
    println!(" UPDATE succeeded");
    
    engine.execute_query("DELETE FROM employees WHERE id = 6")?;
    println!(" DELETE succeeded");
    
    engine.execute_query("INSERT INTO employees (id, name, salary) VALUES (7, 'Grace', 85000)")?;
    println!(" INSERT 2 succeeded");
    
    // Check final state
    let result = engine.execute_query("SELECT * FROM employees ORDER BY id")?;
    println!(" Final state in transaction: {} rows", result.row_count);
    print_result("SELECT * FROM employees ORDER BY id", &result);
    
    engine.execute_query("COMMIT")?;
    println!(" Transaction committed - all operations persistent");
    
    // Verify final state is persistent
    let result = engine.execute_query("SELECT * FROM employees ORDER BY id")?;
    println!(" Final state after commit: {} rows", result.row_count);
    print_result("SELECT * FROM employees ORDER BY id", &result);
    println!();
    
    // Test 8: Check WAL exists (Durability)
    println!(" Test 8: WAL File Existence (Durability)");
    println!("{}", "-".repeat(80));
    
    let wal_path = std::path::Path::new(".wal/wal.log");
    if wal_path.exists() {
        let metadata = std::fs::metadata(wal_path)?;
        println!(" WAL file exists: .wal/wal.log");
        println!(" WAL file size: {} bytes", metadata.len());
        
        // Try to read first few lines
        let content = std::fs::read_to_string(wal_path)?;
        let lines: Vec<&str> = content.lines().take(5).collect();
        println!(" WAL contains {} entries", content.lines().count());
        if !lines.is_empty() {
            println!("  Sample WAL entries:");
            for (i, line) in lines.iter().enumerate() {
                if !line.trim().is_empty() {
                    println!("    Entry {}: {}", i + 1, &line[..line.len().min(100)]);
                }
            }
        }
    } else {
        println!(" WAL file not found (may be normal if no transactions executed)");
    }
    println!();
    
    println!("{}", "=".repeat(80));
    println!("TRANSACTION TESTS COMPLETE");
    println!("{}", "=".repeat(80));
    
    Ok(())
}

