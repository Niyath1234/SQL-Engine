/// Quick test to verify dtype fix for aggregates
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;

fn create_test_data() -> (Vec<String>, Vec<Arc<dyn Array>>) {
    let ids = vec![1, 2, 3, 4];
    let departments = vec!["A", "A", "B", "B"];
    let salaries = vec![100.0, 200.0, 150.0, 250.0];
    
    let id_array: Arc<dyn Array> = Arc::new(Int64Array::from(ids));
    let dept_array: Arc<dyn Array> = Arc::new(StringArray::from(departments));
    let salary_array: Arc<dyn Array> = Arc::new(Float64Array::from(salaries));
    
    let column_names = vec!["id".to_string(), "department".to_string(), "salary".to_string()];
    let columns = vec![id_array, dept_array, salary_array];
    
    (column_names, columns)
}

fn create_fragments(column_names: Vec<String>, columns: Vec<Arc<dyn Array>>) -> Vec<(String, hypergraph_sql_engine::storage::fragment::ColumnFragment)> {
    let mut fragments = Vec::new();
    
    for (name, array) in column_names.iter().zip(columns.iter()) {
        let row_count = array.len();
        let fragment = hypergraph_sql_engine::storage::fragment::ColumnFragment::new(
            array.clone(),
            hypergraph_sql_engine::storage::fragment::FragmentMetadata {
                row_count,
                min_value: None,
                max_value: None,
                cardinality: row_count,
                compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
                memory_size: 0,
            },
        );
        fragments.push((name.clone(), fragment));
    }
    
    fragments
}

fn main() -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("DTYPE FIX VERIFICATION TEST");
    println!("{}", "=".repeat(80));
    
    let mut engine = HypergraphSQLEngine::new();
    
    let (col_names, columns) = create_test_data();
    let fragments = create_fragments(col_names, columns);
    engine.load_table("test", fragments)?;
    println!(" Loaded test table (4 rows)\n");
    
    // Test 1: Simple COUNT
    println!("Test 1: COUNT(*) - should return Int64Array");
    match engine.execute_query("SELECT COUNT(*) as cnt FROM test") {
        Ok(result) => {
            println!("    Query executed successfully");
            println!("   Rows: {}", result.row_count);
            if !result.batches.is_empty() {
                let batch = &result.batches[0].batch;
                println!("   Schema fields: {:?}", batch.schema.fields().iter().map(|f| format!("{}: {:?}", f.name(), f.data_type())).collect::<Vec<_>>());
                for (i, field) in batch.schema.fields().iter().enumerate() {
                    let col = &batch.columns[i];
                    println!("   Column '{}': type = {:?}, len = {}, is_nullable = {}", 
                        field.name(), col.data_type(), col.len(), field.is_nullable());
                    if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                        println!("    Correctly stored as Int64Array");
                        for j in 0..col.len().min(5) {
                            if int_arr.is_null(j) {
                                println!("     Row {}: NULL", j);
                            } else {
                                println!("     Row {}: {}", j, int_arr.value(j));
                            }
                        }
                    } else if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                        println!("   — INCORRECT: Stored as StringArray instead of Int64Array!");
                    } else {
                        println!("   Column type: {:?}", col.data_type());
                    }
                }
            }
        }
        Err(e) => {
            println!("   — FAILED: {}", e);
        }
    }
    
    // Test 2: GROUP BY with COUNT
    println!("\n\nTest 2: GROUP BY COUNT(*) - should return Int64Array");
    match engine.execute_query("SELECT department, COUNT(*) as cnt FROM test GROUP BY department") {
        Ok(result) => {
            println!("    Query executed successfully");
            println!("   Rows: {}", result.row_count);
            if !result.batches.is_empty() {
                let batch = &result.batches[0].batch;
                for (i, field) in batch.schema.fields().iter().enumerate() {
                    let col = &batch.columns[i];
                    println!("   Column '{}': type = {:?}", field.name(), col.data_type());
                    if field.name() == "cnt" {
                        if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                            println!("    Correctly stored as Int64Array");
                            for j in 0..col.len().min(5) {
                                if int_arr.is_null(j) {
                                    println!("     Row {}: NULL", j);
                                } else {
                                    println!("     Row {}: {}", j, int_arr.value(j));
                                }
                            }
                        } else {
                            println!("   — INCORRECT: Stored as {:?} instead of Int64Array!", col.data_type());
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("   — FAILED: {}", e);
        }
    }
    
    // Test 3: HAVING with COUNT
    println!("\n\nTest 3: HAVING COUNT(*) > 1 - should compare Int64 values");
    match engine.execute_query("SELECT department, COUNT(*) as cnt FROM test GROUP BY department HAVING COUNT(*) > 1") {
        Ok(result) => {
            println!("    Query executed successfully");
            println!("   Rows: {}", result.row_count);
            if result.row_count > 0 {
                println!("    HAVING clause working - returned {} rows", result.row_count);
            } else {
                println!("     No rows returned (might be correct if no groups have count > 1)");
            }
        }
        Err(e) => {
            println!("   — FAILED: {}", e);
        }
    }
    
    println!("\n{}", "=".repeat(80));
    
    Ok(())
}

