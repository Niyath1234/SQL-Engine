use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType};
use std::time::Instant;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} <csv_file> [query1] [query2] ...", args[0]);
        eprintln!("Example: {} data.csv \"SELECT * FROM table LIMIT 10\"", args[0]);
        std::process::exit(1);
    }
    
    let csv_path = &args[1];
    let queries = if args.len() > 2 {
        args[2..].to_vec()
    } else {
        vec![
            "SELECT * FROM enterprise_survey LIMIT 10".to_string(),
            "SELECT * FROM enterprise_survey LIMIT 100".to_string(),
            "SELECT * FROM enterprise_survey LIMIT 1000".to_string(),
        ]
    };
    
    println!("🚀 Hypergraph SQL Engine Benchmark");
    println!("===================================");
    println!("Loading data from: {}", csv_path);
    
    // Load CSV data
    let load_start = Instant::now();
    let (rows, columns, fragments) = load_csv(csv_path)?;
    let load_time = load_start.elapsed();
    println!("✓ Loaded {} rows, {} columns in {:?}", rows, columns.len(), load_time);
    
    // Create engine and load table
    let mut engine = HypergraphSQLEngine::new();
    let table_load_start = Instant::now();
    engine.load_table("enterprise_survey", fragments)?;
    let table_load_time = table_load_start.elapsed();
    println!("✓ Table loaded into hypergraph in {:?}", table_load_time);
    
    println!("\n📊 Running Queries:");
    println!("{}", "=".repeat(80));
    
    let mut total_hg_time = 0.0;
    let mut query_count = 0;
    
    for (i, query) in queries.iter().enumerate() {
        println!("\nQuery {}: {}", i + 1, query);
        println!("{}", "-".repeat(80));
        
        // Warmup run (not counted)
        let _ = engine.execute_query(query);
        
        // Benchmark run
        let bench_start = Instant::now();
        match engine.execute_query(query) {
            Ok(result) => {
                let bench_time = bench_start.elapsed();
                let time_ms = bench_time.as_secs_f64() * 1000.0;
                total_hg_time += time_ms;
                query_count += 1;
                
                println!("✓ Success");
                println!("  Rows: {}", result.row_count);
                println!("  Time: {:.2}ms ({:.2}μs)", time_ms, time_ms * 1000.0);
                if time_ms > 0.0 {
                    println!("  Throughput: {:.2} rows/sec", result.row_count as f64 / (time_ms / 1000.0));
                }
                println!("  Execution time (from engine): {:.2}ms", result.execution_time_ms);
            }
            Err(e) => {
                let bench_time = bench_start.elapsed();
                eprintln!("✗ Error: {}", e);
                eprintln!("  Time: {:?}", bench_time);
            }
        }
    }
    
    println!("\n{}", "=".repeat(80));
    println!("📈 Summary:");
    if query_count > 0 {
        println!("  Queries executed: {}", query_count);
        println!("  Total time: {:.2}ms", total_hg_time);
        println!("  Average time: {:.2}ms", total_hg_time / query_count as f64);
    }
    
    Ok(())
}

fn load_csv(path: &str) -> Result<(usize, Vec<String>, Vec<(String, ColumnFragment)>), Box<dyn std::error::Error>> {
    use arrow::array::*;
    use std::sync::Arc;
    
    let mut reader = csv::Reader::from_path(path)?;
    let headers = reader.headers()?.clone();
    let column_names: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    
    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result?;
        rows.push(record);
    }
    
    // Create arrays for each column
    let mut column_arrays: Vec<Arc<dyn arrow::array::Array>> = vec![];
    
    for (col_idx, _col_name) in column_names.iter().enumerate() {
        let mut int_values = Vec::new();
        let mut float_values = Vec::new();
        let mut string_values = Vec::new();
        let mut is_int = true;
        let mut is_float = true;
        
        for row in rows.iter() {
            if let Some(field) = row.get(col_idx) {
                if field.is_empty() {
                    if is_int {
                        int_values.push(None);
                    } else if is_float {
                        float_values.push(None);
                    } else {
                        string_values.push(None);
                    }
                    continue;
                }
                if is_int {
                    match field.parse::<i64>() {
                        Ok(v) => int_values.push(Some(v)),
                        Err(_) => {
                            is_int = false;
                            if is_float {
                                match field.parse::<f64>() {
                                    Ok(v) => float_values.push(Some(v)),
                                    Err(_) => {
                                        is_float = false;
                                        string_values.push(Some(field.to_string()));
                                    }
                                }
                            } else {
                                string_values.push(Some(field.to_string()));
                            }
                        }
                    }
                } else if is_float {
                    match field.parse::<f64>() {
                        Ok(v) => float_values.push(Some(v)),
                        Err(_) => {
                            is_float = false;
                            string_values.push(Some(field.to_string()));
                        }
                    }
                } else {
                    string_values.push(Some(field.to_string()));
                }
            }
        }
        
        // Fill remaining with None if needed
        while is_int && int_values.len() < rows.len() {
            int_values.push(None);
        }
        while is_float && !is_int && float_values.len() < rows.len() {
            float_values.push(None);
        }
        while !is_int && !is_float && string_values.len() < rows.len() {
            string_values.push(None);
        }
        
        let array: Arc<dyn arrow::array::Array> = if is_int && !int_values.is_empty() {
            Arc::new(Int64Array::from(int_values))
        } else if is_float && !float_values.is_empty() {
            Arc::new(Float64Array::from(float_values))
        } else {
            Arc::new(StringArray::from(string_values))
        };
        
        column_arrays.push(array);
    }
    
    // Create fragments
    let mut column_fragments = Vec::new();
    for (col_idx, col_name) in column_names.iter().enumerate() {
        let fragment = ColumnFragment::new(
            column_arrays[col_idx].clone(),
            FragmentMetadata {
                row_count: rows.len(),
                min_value: None,
                max_value: None,
                cardinality: rows.len(),
                compression: CompressionType::None,
                memory_size: 0,
            },
        );
        column_fragments.push((col_name.clone(), fragment));
    }
    
    Ok((rows.len(), column_names, column_fragments))
}

