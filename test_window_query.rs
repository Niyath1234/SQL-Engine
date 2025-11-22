/// Test Window Functions - Check current implementation status
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

fn main() -> anyhow::Result<()> {
    println!("{}", "=".repeat(80));
    println!(" Testing Window Functions - Implementation Status");
    println!("{}", "=".repeat(80));
    
    // Initialize engine
    println!("\n Initializing Hypergraph SQL Engine...");
    let mut engine = HypergraphSQLEngine::new();
    
    // Load CSV data
    let csv_path = "annual-enterprise-survey-2024-financial-year-provisional.csv";
    println!("\n Loading CSV: {}", csv_path);
    let fragments = load_csv(csv_path)?;
    println!("   Loaded {} columns", fragments.len());
    
    // Load table
    println!("\n Loading table into engine...");
    engine.load_table("survey", fragments)?;
    println!("    Table 'survey' loaded successfully");
    
    println!("\n{}", "=".repeat(80));
    println!("WINDOW FUNCTION TESTS");
    println!("{}", "=".repeat(80));
    
    // Test 1: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
    println!("\n Test 1: ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Value)");
    println!("   Query: SELECT Year, Value, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Value) as rn FROM survey LIMIT 10");
    
    let query1 = "SELECT Year, Value, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Value) as rn FROM survey LIMIT 10";
    match engine.execute_query(query1) {
        Ok(result) => {
            println!("    Query executed successfully!");
            println!("   Rows returned: {}", result.row_count);
            println!("   Execution time: {:.2} ms", result.execution_time_ms);
            println!("     Note: Window function execution is placeholder (passes through input)");
        }
        Err(e) => {
            println!("    Error: {}", e);
            println!("     This is expected - window function extraction/execution not fully implemented yet");
        }
    }
    
    // Test 2: SUM() OVER (PARTITION BY ...)
    println!("\n Test 2: SUM() OVER (PARTITION BY Year)");
    println!("   Query: SELECT Year, Value, SUM(CAST(Value AS INTEGER)) OVER (PARTITION BY Year) as total FROM survey LIMIT 10");
    
    let query2 = "SELECT Year, Value, SUM(CAST(Value AS INTEGER)) OVER (PARTITION BY Year) as total FROM survey LIMIT 10";
    match engine.execute_query(query2) {
        Ok(result) => {
            println!("    Query executed successfully!");
            println!("   Rows returned: {}", result.row_count);
            println!("   Execution time: {:.2} ms", result.execution_time_ms);
            println!("     Note: Window function execution is placeholder (passes through input)");
        }
        Err(e) => {
            println!("    Error: {}", e);
            println!("     This is expected - window function extraction/execution not fully implemented yet");
        }
    }
    
    // Test 3: RANK() OVER (ORDER BY ...)
    println!("\n Test 3: RANK() OVER (ORDER BY Value DESC)");
    println!("   Query: SELECT Year, Value, RANK() OVER (ORDER BY Value DESC) as rank FROM survey LIMIT 10");
    
    let query3 = "SELECT Year, Value, RANK() OVER (ORDER BY Value DESC) as rank FROM survey LIMIT 10";
    match engine.execute_query(query3) {
        Ok(result) => {
            println!("    Query executed successfully!");
            println!("   Rows returned: {}", result.row_count);
            println!("   Execution time: {:.2} ms", result.execution_time_ms);
            println!("     Note: Window function execution is placeholder (passes through input)");
        }
        Err(e) => {
            println!("    Error: {}", e);
            println!("     This is expected - window function extraction/execution not fully implemented yet");
        }
    }
    
    println!("\n{}", "=".repeat(80));
    println!("IMPLEMENTATION STATUS");
    println!("{}", "=".repeat(80));
    
    println!("\n Completed:");
    println!("    Window function data structures (WindowFunctionInfo, WindowFunctionType)");
    println!("    Window operator in query plan (PlanOperator::Window)");
    println!("    Window function types (ROW_NUMBER, RANK, SUM OVER, etc.)");
    println!("    Window operator cost/cardinality estimation");
    println!("    Window operator placeholder in execution");
    println!("    Test suite for window function infrastructure");
    
    println!("\n In Progress:");
    println!("    Window function extraction from SQL parser");
    println!("    Window operator integration in planner");
    
    println!("\n TODO:");
    println!("    Complete window function detection from sqlparser AST");
    println!("    Implement WindowOperator execution logic:");
    println!("     - Partition-based grouping");
    println!("     - ORDER BY sorting within partitions");
    println!("     - ROW_NUMBER() implementation");
    println!("     - RANK() / DENSE_RANK() implementation");
    println!("     - SUM() OVER() / AVG() OVER() implementation");
    println!("     - Frame specification (ROWS BETWEEN ...)");
    
    println!("\n{}", "=".repeat(80));
    println!(" Infrastructure ready - Execution pending");
    println!("{}", "=".repeat(80));
    
    Ok(())
}

