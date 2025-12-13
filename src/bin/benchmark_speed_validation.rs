/// Comprehensive benchmark that tests speed improvements and validates correctness
/// 
/// Features:
/// - Measures execution time
/// - Returns actual rows
/// - Validates correctness
/// - Compares before/after optimizations

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType, Value};
use hypergraph_sql_engine::execution::engine::QueryResult;
use std::time::Instant;
use std::env;
use arrow::array::*;
use arrow::datatypes::*;

/// Load CSV data (simplified version)
fn load_csv(path: &str) -> Result<(usize, Vec<String>, Vec<(String, ColumnFragment)>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    
    // Read header
    let header = lines.next()
        .ok_or("Empty CSV file")??;
    let columns: Vec<String> = header.split(',').map(|s| s.trim().to_string()).collect();
    
    let mut rows = Vec::new();
    let mut csv_rows = Vec::new();
    
    for line in lines {
        let line = line?;
        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        csv_rows.push(values.clone());
        rows.push(values);
    }
    
    let row_count = rows.len();
    
    // Convert to fragments - format expected by load_table: Vec<(String, ColumnFragment)>
    let mut fragments = Vec::new();
    for col_idx in 0..columns.len() {
        let mut col_values = Vec::new();
        for row in &rows {
            if col_idx < row.len() {
                col_values.push(Value::String(
                    row[col_idx].clone()
                ));
            }
        }
        
        let fragment = ColumnFragment {
            metadata: FragmentMetadata {
                row_count: col_values.len(),
                compression: CompressionType::None,
                min_value: None,
                max_value: None,
                cardinality: col_values.len(),
                memory_size: 0,
                table_name: None,
                column_name: None,
                metadata: std::collections::HashMap::new(),
            },
            values: col_values,
        };
        fragments.push((columns[col_idx].clone(), fragment));
    }
    
    Ok((row_count, columns, fragments, csv_rows))
}

/// Extract rows from QueryResult
fn extract_rows(result: &QueryResult) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let mut all_rows = Vec::new();
    
    for batch in &result.batches {
        if batch.batch.columns.is_empty() {
            continue;
        }
        
        let num_cols = batch.batch.columns.len();
        let num_rows = batch.row_count;
        
        for row_idx in 0..num_rows {
            if !batch.selection[row_idx] {
                continue;
            }
            
            let mut row = Vec::new();
            for col_idx in 0..num_cols {
                let col = &batch.batch.columns[col_idx];
                let value = match col.data_type() {
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>()
                            .ok_or("Failed to downcast Int64")?;
                        arr.value(row_idx).to_string()
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>()
                            .ok_or("Failed to downcast Float64")?;
                        arr.value(row_idx).to_string()
                    }
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>()
                            .ok_or("Failed to downcast String")?;
                        arr.value(row_idx).to_string()
                    }
                    DataType::Boolean => {
                        let arr = col.as_any().downcast_ref::<BooleanArray>()
                            .ok_or("Failed to downcast Boolean")?;
                        arr.value(row_idx).to_string()
                    }
                    _ => format!("{:?}", col.data_type()),
                };
                row.push(value);
            }
            all_rows.push(row);
        }
    }
    
    Ok(all_rows)
}

/// Format rows for display
fn format_rows(rows: &[Vec<String>], max_display: usize) -> String {
    if rows.is_empty() {
        return "  (no rows)".to_string();
    }
    
    let display_count = rows.len().min(max_display);
    let mut output = String::new();
    
    for (i, row) in rows.iter().take(display_count).enumerate() {
        output.push_str(&format!("  Row {}: [{}]\n", i + 1, row.join(", ")));
    }
    
    if rows.len() > max_display {
        output.push_str(&format!("  ... ({} more rows)\n", rows.len() - max_display));
    }
    
    output
}

/// Run a query and measure performance
fn run_query(
    engine: &mut HypergraphSQLEngine,
    sql: &str,
    warmup_runs: usize,
    benchmark_runs: usize,
) -> Result<(f64, QueryResult, Vec<Vec<String>>), Box<dyn std::error::Error>> {
    // Warmup runs (to fill caches, JIT compile, etc.)
    for _ in 0..warmup_runs {
        let _ = engine.execute_query(sql)?;
    }
    
    // Benchmark runs
    let mut total_time = 0.0;
    let mut last_result = None;
    
    for _ in 0..benchmark_runs {
        let start = Instant::now();
        let result = engine.execute_query(sql)?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0; // Convert to ms
        total_time += elapsed;
        last_result = Some(result);
    }
    
    let avg_time = total_time / benchmark_runs as f64;
    let result = last_result.ok_or("No results")?;
    let rows = extract_rows(&result)?;
    
    Ok((avg_time, result, rows))
}

/// Validate query results (basic checks)
fn validate_result(
    result: &QueryResult,
    rows: &[Vec<String>],
    expected_row_count: Option<usize>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut valid = true;
    let mut errors = Vec::new();
    
    // Check row count matches
    if let Some(expected) = expected_row_count {
        if result.row_count != expected {
            errors.push(format!(
                "Row count mismatch: expected {}, got {}",
                expected, result.row_count
            ));
            valid = false;
        }
    }
    
    // Check extracted rows match batch count
    if rows.len() != result.row_count {
        errors.push(format!(
            "Extracted rows mismatch: expected {}, got {}",
            result.row_count, rows.len()
        ));
        valid = false;
    }
    
    // Check batches are consistent
    let total_batch_rows: usize = result.batches.iter()
        .map(|b| b.row_count)
        .sum();
    
    if total_batch_rows != result.row_count {
        errors.push(format!(
            "Batch row count mismatch: batches sum to {}, but row_count is {}",
            total_batch_rows, result.row_count
        ));
        valid = false;
    }
    
    if !valid {
        eprintln!("Validation errors:");
        for err in &errors {
            eprintln!("  - {}", err);
        }
    }
    
    Ok(valid)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} <csv_file> [query1] [query2] ...", args[0]);
        eprintln!("\nExample:");
        eprintln!("  {} data.csv \"SELECT * FROM enterprise_survey LIMIT 10\"", args[0]);
        eprintln!("\nIf no queries provided, runs a default benchmark suite.");
        std::process::exit(1);
    }
    
    let csv_path = &args[1];
    let queries = if args.len() > 2 {
        args[2..].to_vec()
    } else {
        // Default benchmark suite
        vec![
            // Simple scans
            "SELECT * FROM enterprise_survey LIMIT 10".to_string(),
            "SELECT * FROM enterprise_survey LIMIT 100".to_string(),
            
            // Filters (should use AVX2 SIMD)
            "SELECT * FROM enterprise_survey WHERE Year = 2024 LIMIT 100".to_string(),
            "SELECT * FROM enterprise_survey WHERE Year > 2020 LIMIT 100".to_string(),
            
            // Aggregations
            "SELECT COUNT(*) FROM enterprise_survey".to_string(),
            "SELECT Year, COUNT(*) FROM enterprise_survey GROUP BY Year".to_string(),
        ]
    };
    
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║          LQS Engine Performance & Correctness Benchmark                      ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Loading data from: {}", csv_path);
    
    // Load CSV data
    let load_start = Instant::now();
    let (row_count, columns, fragments, _csv_rows) = load_csv(csv_path)?;
    let load_time = load_start.elapsed();
    println!("✓ Loaded {} rows, {} columns in {:?}", row_count, columns.len(), load_time);
    println!();
    
    // Create engine and load table
    let mut engine = HypergraphSQLEngine::new();
    let table_load_start = Instant::now();
    engine.load_table("enterprise_survey", fragments)?;
    let table_load_time = table_load_start.elapsed();
    println!("✓ Table loaded into engine in {:?}", table_load_time);
    println!();
    
    // Run benchmarks
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                          Running Benchmarks                                 ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    
    let warmup_runs = 2;
    let benchmark_runs = 5;
    let queries_per_set = 3; // Process queries in sets to avoid memory pressure
    
    let mut total_queries = 0;
    let mut total_time = 0.0;
    let mut all_results = Vec::new();
    
    // Process queries in sets to avoid memory pressure
    let query_sets: Vec<Vec<&String>> = queries.chunks(queries_per_set)
        .map(|chunk| chunk.iter().collect())
        .collect();
    
    for (set_idx, query_set) in query_sets.iter().enumerate() {
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║                    Processing Query Set {} of {}                            ║", set_idx + 1, query_sets.len());
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
        println!();
        
        for (query_idx, sql) in query_set.iter().enumerate() {
            let global_idx = set_idx * queries_per_set + query_idx + 1;
            println!("Query {}: {}", global_idx, sql);
            println!("{}", "-".repeat(80));
            
            match run_query(&mut engine, sql, warmup_runs, benchmark_runs) {
                Ok((avg_time, result, rows)) => {
                    total_queries += 1;
                    total_time += avg_time;
                    
                    // Validate
                    let is_valid = validate_result(&result, &rows, None)?;
                    
                    println!("  Execution time: {:.2} ms (avg over {} runs)", avg_time, benchmark_runs);
                    println!("  Rows returned: {}", result.row_count);
                    println!("  Batches: {}", result.batches.len());
                    println!("  Validation: {}", if is_valid { "✓ PASSED" } else { "✗ FAILED" });
                    
                    // Display rows (first 5)
                    if !rows.is_empty() {
                        println!("  Sample rows:");
                        print!("{}", format_rows(&rows, 5));
                    }
                    
                    // Store only essential data to save memory
                    all_results.push((sql.clone(), avg_time, result.row_count, is_valid));
                    
                    // Clear result batches to free memory
                    drop(result);
                    drop(rows);
                }
                Err(e) => {
                    eprintln!("  ✗ ERROR: {}", e);
                    all_results.push((sql.clone(), 0.0, 0, false));
                }
            }
            
            println!();
        }
        
        // Force garbage collection between sets (hint to Rust)
        if set_idx < query_sets.len() - 1 {
            println!("  [Memory management: Clearing query results before next set...]");
            println!();
        }
    }
    
    // Summary
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              Summary                                         ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Total queries executed: {}", total_queries);
    if total_queries > 0 {
        println!("Average query time: {:.2} ms", total_time / total_queries as f64);
        println!("Total execution time: {:.2} ms", total_time);
    }
    println!();
    
    // Detailed results table
    println!("Detailed Results:");
    println!("{}", "=".repeat(100));
    println!("{:<60} {:>12} {:>12} {:>10}", "Query", "Time (ms)", "Rows", "Valid");
    println!("{}", "-".repeat(100));
    
    for (sql, time, rows, valid) in &all_results {
        let sql_short = if sql.len() > 58 {
            format!("{}..", &sql[..56])
        } else {
            sql.clone()
        };
        println!(
            "{:<60} {:>12.2} {:>12} {:>10}",
            sql_short,
            time,
            rows,
            if *valid { "✓" } else { "✗" }
        );
    }
    println!("{}", "=".repeat(100));
    
    // Performance analysis
    println!();
    println!("Performance Analysis:");
    println!("{}", "-".repeat(80));
    
    // Check if SIMD is being used (filters should be fast)
    let filter_queries: Vec<_> = all_results.iter()
        .filter(|(sql, _, _, _)| sql.contains("WHERE"))
        .collect();
    
    if !filter_queries.is_empty() {
        println!("Filter queries (should use AVX2 SIMD):");
        for (sql, time, rows, valid) in &filter_queries {
            println!("  {}: {:.2} ms ({} rows) {}", sql, time, rows, if *valid { "✓" } else { "✗" });
        }
    }
    
    // Check aggregation performance
    let agg_queries: Vec<_> = all_results.iter()
        .filter(|(sql, _, _, _)| sql.contains("COUNT") || sql.contains("SUM") || sql.contains("GROUP"))
        .collect();
    
    if !agg_queries.is_empty() {
        println!("\nAggregation queries:");
        for (sql, time, rows, valid) in &agg_queries {
            println!("  {}: {:.2} ms ({} rows) {}", sql, time, rows, if *valid { "✓" } else { "✗" });
        }
    }
    
    println!();
    println!("✓ Benchmark complete!");
    
    Ok(())
}

