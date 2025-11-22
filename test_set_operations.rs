/// Test Set Operations (UNION, INTERSECT, EXCEPT)
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::ColumnFragment;
use hypergraph_sql_engine::storage::fragment::Value;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

fn create_test_data() -> (HypergraphSQLEngine, String, String) {
    let mut engine = HypergraphSQLEngine::new();
    
    // Create table1
    let col1_id = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let col1_name = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]));
    
    let fragment_id1 = ColumnFragment::new(
        col1_id,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 5,
            min_value: Some(Value::Int64(1)),
            max_value: Some(Value::Int64(5)),
            cardinality: 5,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 40,
        },
    );
    
    let fragment_name1 = ColumnFragment::new(
        col1_name,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 5,
            min_value: None,
            max_value: None,
            cardinality: 5,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 100,
        },
    );
    
    engine.load_table("table1", vec![
        ("id".to_string(), fragment_id1),
        ("name".to_string(), fragment_name1),
    ]).unwrap();
    
    // Create table2
    let col2_id = Arc::new(Int64Array::from(vec![3, 4, 5, 6, 7]));
    let col2_name = Arc::new(StringArray::from(vec!["Charlie", "David", "Eve", "Frank", "Grace"]));
    
    let fragment_id2 = ColumnFragment::new(
        col2_id,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 5,
            min_value: Some(Value::Int64(3)),
            max_value: Some(Value::Int64(7)),
            cardinality: 5,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 40,
        },
    );
    
    let fragment_name2 = ColumnFragment::new(
        col2_name,
        hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: 5,
            min_value: None,
            max_value: None,
            cardinality: 5,
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: 100,
        },
    );
    
    engine.load_table("table2", vec![
        ("id".to_string(), fragment_id2),
        ("name".to_string(), fragment_name2),
    ]).unwrap();
    
    (engine, "table1".to_string(), "table2".to_string())
}

/// Print batch contents in a readable format
fn print_batch_contents(batch: &hypergraph_sql_engine::execution::batch::ExecutionBatch, max_rows: usize) {
    let schema = &batch.batch.schema;
    let columns = &batch.batch.columns;
    
    if columns.is_empty() || schema.fields().is_empty() {
        println!("  (Empty batch)");
        return;
    }
    
    // Print header
    let headers: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
    println!("  Columns: {}", headers.join(", "));
    println!("  Rows:");
    
    // Print rows
    let row_count = batch.row_count.min(max_rows);
    for row_idx in 0..row_count {
        if !batch.selection[row_idx] {
            continue;
        }
        
        let mut values = Vec::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if col_idx < columns.len() {
                let col = &columns[col_idx];
                let val_str = if col.len() > row_idx {
                    if col.is_null(row_idx) {
                        "NULL".to_string()
                    } else {
                        match col.data_type() {
                            arrow::datatypes::DataType::Int64 => {
                                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                format!("{}", arr.value(row_idx))
                            }
                            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                format!("\"{}\"", arr.value(row_idx))
                            }
                            _ => format!("{:?}", col),
                        }
                    }
                } else {
                    "N/A".to_string()
                };
                values.push(val_str);
            }
        }
        println!("    Row {}: {}", row_idx + 1, values.join(" | "));
    }
    
    if batch.row_count > max_rows {
        println!("  ... ({} more rows)", batch.row_count - max_rows);
    }
}

fn main() {
    println!("{}", "=".repeat(80));
    println!("=== Testing Set Operations ===\n");
    
    let (mut engine, table1, table2) = create_test_data();
    
    // Show input data
    println!(" INPUT DATA:");
    println!("{}", "-".repeat(80));
    println!("Table: {}", table1);
    let t1_result = engine.execute_query(&format!("SELECT id, name FROM {} ORDER BY id", table1)).unwrap();
    println!("  Rows: {}", t1_result.row_count);
    for batch in &t1_result.batches {
        print_batch_contents(batch, 10);
    }
    println!();
    
    println!("Table: {}", table2);
    let t2_result = engine.execute_query(&format!("SELECT id, name FROM {} ORDER BY id", table2)).unwrap();
    println!("  Rows: {}", t2_result.row_count);
    for batch in &t2_result.batches {
        print_batch_contents(batch, 10);
    }
    println!();
    
    println!("{}", "=".repeat(80));
    
    // Test 1: UNION ALL
    println!("\n Test 1: UNION ALL");
    println!("{}", "-".repeat(80));
    println!("Query: SELECT id, name FROM {} UNION ALL SELECT id, name FROM {}", table1, table2);
    println!("Expected: All 10 rows (5 from table1 + 5 from table2, duplicates allowed)");
    match engine.execute_query(&format!("SELECT id, name FROM {} UNION ALL SELECT id, name FROM {}", table1, table2)) {
        Ok(result) => {
            println!(" UNION ALL succeeded: {} rows", result.row_count);
            for (i, batch) in result.batches.iter().enumerate() {
                println!("  Batch {}: {} rows", i, batch.row_count);
                print_batch_contents(batch, 20);
            }
        }
        Err(e) => println!("— UNION ALL failed: {}", e),
    }
    println!();
    
    // Test 2: UNION (distinct)
    println!(" Test 2: UNION (distinct)");
    println!("{}", "-".repeat(80));
    println!("Query: SELECT id, name FROM {} UNION SELECT id, name FROM {}", table1, table2);
    println!("Expected: 7 unique rows (removes duplicates: (3,Charlie), (4,David), (5,Eve))");
    match engine.execute_query(&format!("SELECT id, name FROM {} UNION SELECT id, name FROM {}", table1, table2)) {
        Ok(result) => {
            println!(" UNION succeeded: {} rows", result.row_count);
            for (i, batch) in result.batches.iter().enumerate() {
                println!("  Batch {}: {} rows", i, batch.row_count);
                print_batch_contents(batch, 20);
            }
        }
        Err(e) => println!("— UNION failed: {}", e),
    }
    println!();
    
    // Test 3: INTERSECT
    println!(" Test 3: INTERSECT");
    println!("{}", "-".repeat(80));
    println!("Query: SELECT id, name FROM {} INTERSECT SELECT id, name FROM {}", table1, table2);
    println!("Expected: 3 rows (common rows: (3,Charlie), (4,David), (5,Eve))");
    match engine.execute_query(&format!("SELECT id, name FROM {} INTERSECT SELECT id, name FROM {}", table1, table2)) {
        Ok(result) => {
            println!(" INTERSECT succeeded: {} rows", result.row_count);
            for (i, batch) in result.batches.iter().enumerate() {
                println!("  Batch {}: {} rows", i, batch.row_count);
                print_batch_contents(batch, 20);
            }
        }
        Err(e) => println!("— INTERSECT failed: {}", e),
    }
    println!();
    
    // Test 4: EXCEPT
    println!(" Test 4: EXCEPT");
    println!("{}", "-".repeat(80));
    println!("Query: SELECT id, name FROM {} EXCEPT SELECT id, name FROM {}", table1, table2);
    println!("Expected: 2 rows (rows in table1 but not in table2: (1,Alice), (2,Bob))");
    match engine.execute_query(&format!("SELECT id, name FROM {} EXCEPT SELECT id, name FROM {}", table1, table2)) {
        Ok(result) => {
            println!(" EXCEPT succeeded: {} rows", result.row_count);
            for (i, batch) in result.batches.iter().enumerate() {
                println!("  Batch {}: {} rows", i, batch.row_count);
                print_batch_contents(batch, 20);
            }
        }
        Err(e) => println!("— EXCEPT failed: {}", e),
    }
    println!();
    
    println!("{}", "=".repeat(80));
    println!("=== Set Operations Tests Complete ===");
    println!("{}", "=".repeat(80));
}

