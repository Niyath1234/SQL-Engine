/// Test script to run queries and show output for verification
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use std::fs::File;
use csv::ReaderBuilder;
use arrow::array::*;
use std::sync::Arc;

fn load_csv(path: &str) -> anyhow::Result<Vec<(String, hypergraph_sql_engine::storage::fragment::ColumnFragment)>> {
    let file = File::open(path)?;
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);
    let headers = reader.headers()?.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let mut rows = Vec::new();
    
    for result in reader.records() {
        rows.push(result?.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    }
    
    let mut fragments = Vec::new();
    for (col_idx, col_name) in headers.iter().enumerate() {
        let mut int_vals: Vec<Option<i64>> = Vec::new();
        let mut float_vals: Vec<Option<f64>> = Vec::new();
        let mut str_vals: Vec<Option<String>> = Vec::new();
        let mut is_int = true;
        let mut is_float = true;
        
        for row in &rows {
            if col_idx < row.len() {
                let val = &row[col_idx];
                if val.is_empty() {
                    int_vals.push(None);
                    float_vals.push(None);
                    str_vals.push(None);
                } else if let Ok(i) = val.parse::<i64>() {
                    int_vals.push(Some(i));
                    float_vals.push(Some(i as f64));
                    str_vals.push(Some(val.clone()));
                } else if let Ok(f) = val.parse::<f64>() {
                    is_int = false;
                    float_vals.push(Some(f));
                    str_vals.push(Some(val.clone()));
                } else {
                    is_int = false;
                    is_float = false;
                    str_vals.push(Some(val.clone()));
                }
            }
        }
        
        let array: Arc<dyn arrow::array::Array> = if is_int && !int_vals.is_empty() {
            Arc::new(Int64Array::from(int_vals))
        } else if is_float && !float_vals.is_empty() {
            Arc::new(Float64Array::from(float_vals))
        } else {
            Arc::new(StringArray::from(str_vals))
        };
        
        let metadata = hypergraph_sql_engine::storage::fragment::FragmentMetadata {
            row_count: array.len(),
            min_value: None,
            max_value: None,
            cardinality: array.len(),
            compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
            memory_size: array.len() * 8,
        };
        
        let fragment = hypergraph_sql_engine::storage::fragment::ColumnFragment::new(array, metadata);
        fragments.push((col_name.clone(), fragment));
    }
    
    Ok(fragments)
}

fn print_query_result(result: &hypergraph_sql_engine::execution::engine::QueryResult, query: &str) {
    println!("\n{}", "=".repeat(80));
    println!("Query: {}", query);
    println!("{}", "=".repeat(80));
    println!("Rows returned: {}", result.row_count);
    println!("Batches: {}", result.batches.len());
    println!("Execution time: {:.2} ms", result.execution_time_ms);
    println!("{}", "-".repeat(80));
    
    // Convert to RecordBatch and print
    let record_batches = result.to_record_batches();
    if record_batches.is_empty() {
        println!("No results returned.");
        return;
    }
    
    // Print schema
    let schema = record_batches[0].schema();
    print!("\nColumns: ");
    for (i, field) in schema.fields().iter().enumerate() {
        if i > 0 { print!(" | "); }
        print!("{}", field.name());
    }
    println!("\n{}", "-".repeat(80));
    
    // Print data (first 20 rows)
    let mut row_count = 0;
    for batch in &record_batches {
        for row_idx in 0..batch.num_rows() {
            if row_count >= 20 {
                println!("\n... (showing first 20 rows, total: {} rows)", result.row_count);
                return;
            }
            
            print!("Row {}: ", row_count + 1);
            for (col_idx, field) in schema.fields().iter().enumerate() {
                if col_idx > 0 { print!(" | "); }
                
                let column = batch.column(col_idx);
                match column.data_type() {
                    arrow::datatypes::DataType::Int64 => {
                        let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            print!("NULL");
                        } else {
                            print!("{}", arr.value(row_idx));
                        }
                    }
                    arrow::datatypes::DataType::Float64 => {
                        let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            print!("NULL");
                        } else {
                            print!("{:.2}", arr.value(row_idx));
                        }
                    }
                    arrow::datatypes::DataType::Utf8 => {
                        let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                        if arr.is_null(row_idx) {
                            print!("NULL");
                        } else {
                            print!("{}", arr.value(row_idx));
                        }
                    }
                    _ => {
                        print!("{:?}", column);
                    }
                }
            }
            println!();
            row_count += 1;
        }
    }
    
    if row_count < result.row_count {
        println!("\n... (showing first {} rows, total: {} rows)", row_count, result.row_count);
    }
    println!();
}

fn main() -> anyhow::Result<()> {
    println!(" Starting Query Verification Tests");
    println!("{}", "=".repeat(80));
    
    // Initialize engine
    println!("\n¦ Initializing Hypergraph SQL Engine...");
    let mut engine = HypergraphSQLEngine::new();
    
    // Load CSV data
    let csv_path = "annual-enterprise-survey-2024-financial-year-provisional.csv";
    println!("\n‚ Loading CSV: {}", csv_path);
    let fragments = load_csv(csv_path)?;
    println!("   Loaded {} columns", fragments.len());
    
    // Load table
    println!("\n Loading table into engine...");
    engine.load_table("survey", fragments)?;
    println!("    Table 'survey' loaded successfully");
    
    // Test queries
    println!("\n{}", "=".repeat(80));
    println!("TESTING QUERIES");
    println!("{}", "=".repeat(80));
    
    // Query 1: Simple SELECT
    let query1 = "SELECT Year, Value FROM survey LIMIT 10";
    println!("\n Test 1: Simple SELECT");
    match engine.execute_query(query1) {
        Ok(result) => print_query_result(&result, query1),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Query 2: COUNT aggregate
    let query2 = "SELECT COUNT(*) as total_rows FROM survey";
    println!("\n Test 2: COUNT aggregate");
    match engine.execute_query(query2) {
        Ok(result) => print_query_result(&result, query2),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Query 3: GROUP BY with aggregates
    let query3 = "SELECT Year, COUNT(*) as count, SUM(CAST(Value AS INTEGER)) as total_value FROM survey GROUP BY Year LIMIT 10";
    println!("\n Test 3: GROUP BY with aggregates");
    match engine.execute_query(query3) {
        Ok(result) => print_query_result(&result, query3),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Query 4: HAVING clause
    let query4 = "SELECT Year, COUNT(*) as count FROM survey GROUP BY Year HAVING COUNT(*) > 100 LIMIT 10";
    println!("\n Test 4: HAVING clause");
    match engine.execute_query(query4) {
        Ok(result) => print_query_result(&result, query4),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Query 5: ORDER BY
    let query5 = "SELECT Year, Value FROM survey ORDER BY Year DESC LIMIT 10";
    println!("\n Test 5: ORDER BY");
    match engine.execute_query(query5) {
        Ok(result) => print_query_result(&result, query5),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Query 6: WHERE clause
    let query6 = "SELECT Year, Value FROM survey WHERE Year = 2024 LIMIT 10";
    println!("\n Test 6: WHERE clause");
    match engine.execute_query(query6) {
        Ok(result) => print_query_result(&result, query6),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Query 7: AVG aggregate
    let query7 = "SELECT Year, AVG(CAST(Value AS INTEGER)) as avg_value FROM survey GROUP BY Year LIMIT 10";
    println!("\n Test 7: AVG aggregate");
    match engine.execute_query(query7) {
        Ok(result) => print_query_result(&result, query7),
        Err(e) => println!(" Error: {}", e),
    }
    
    // Summary
    println!("\n{}", "=".repeat(80));
    println!(" Query Verification Complete!");
    println!("{}", "=".repeat(80));
    
    // Cache stats
    let stats = engine.cache_stats();
    println!("\n Cache Statistics:");
    println!("   Plan cache size: {}", stats.plan_cache_size);
    println!("   Result cache size: {}", stats.result_cache_size);
    
    Ok(())
}

