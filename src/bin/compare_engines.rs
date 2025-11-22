use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType};
use std::time::Instant;
use std::env;
use tokio_postgres::NoTls;
use duckdb::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        // Comprehensive benchmark suite for all implemented features
        vec![
            // Category 1: Basic SELECT - Full table scan
            "SELECT * FROM enterprise_survey LIMIT 10".to_string(),
            "SELECT * FROM enterprise_survey LIMIT 100".to_string(),
            "SELECT * FROM enterprise_survey LIMIT 1000".to_string(),
            
            // Category 2: Column selection
            "SELECT Year, Value FROM enterprise_survey LIMIT 100".to_string(),
            
            // Category 3: WHERE clause - Basic predicates (=, !=, <, >, <=, >=)
            "SELECT * FROM enterprise_survey WHERE Year = 2024 LIMIT 100".to_string(),
            "SELECT * FROM enterprise_survey WHERE Year > 2020 LIMIT 100".to_string(),
            "SELECT * FROM enterprise_survey WHERE Year >= 2024 LIMIT 100".to_string(),
            
            // Category 4: WHERE with AND/OR
            "SELECT * FROM enterprise_survey WHERE Year = 2024 AND Value > 1000 LIMIT 100".to_string(),
            
            // Category 5: ORDER BY
            "SELECT * FROM enterprise_survey ORDER BY Year LIMIT 100".to_string(),
            "SELECT * FROM enterprise_survey ORDER BY Year DESC LIMIT 100".to_string(),
            
            // Category 6: GROUP BY
            "SELECT Year, COUNT(*) FROM enterprise_survey GROUP BY Year".to_string(),
            
            // Category 7: Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
            "SELECT COUNT(*) FROM enterprise_survey".to_string(),
            "SELECT SUM(Value) FROM enterprise_survey".to_string(),
            "SELECT AVG(Value) FROM enterprise_survey".to_string(),
            "SELECT MIN(Value) FROM enterprise_survey".to_string(),
            "SELECT MAX(Value) FROM enterprise_survey".to_string(),
            "SELECT Year, COUNT(*), SUM(Value), AVG(Value), MIN(Value), MAX(Value) FROM enterprise_survey GROUP BY Year".to_string(),
            
            // Category 8: Combined queries (WHERE + GROUP BY + ORDER BY)
            "SELECT Year, COUNT(*) FROM enterprise_survey WHERE Year = 2024 GROUP BY Year".to_string(),
            "SELECT Year, SUM(Value) FROM enterprise_survey WHERE Year > 2020 GROUP BY Year ORDER BY Year".to_string(),
            
            // Category 9: LIMIT/OFFSET
            "SELECT * FROM enterprise_survey LIMIT 10 OFFSET 0".to_string(),
            "SELECT * FROM enterprise_survey LIMIT 10 OFFSET 100".to_string(),
            
            // Category 10: Complex multi-feature query
            "SELECT Year, Industry_aggregation_NZSIOC, COUNT(*), SUM(Value), AVG(Value) FROM enterprise_survey WHERE Year = 2024 GROUP BY Year, Industry_aggregation_NZSIOC ORDER BY SUM(Value) DESC LIMIT 50".to_string(),
        ]
    };
    
    println!(" Hypergraph SQL Engine vs PostgreSQL Benchmark");
    println!("{}", "=".repeat(80));
    println!("Loading data from: {}", csv_path);
    
    // Load CSV data
    let load_start = Instant::now();
    let (row_count, columns, fragments, csv_rows) = load_csv(csv_path)?;
    let load_time = load_start.elapsed();
    println!(" Loaded {} rows, {} columns in {:?}", row_count, columns.len(), load_time);
    
    // Create hypergraph engine and load table
    let mut hg_engine = HypergraphSQLEngine::new();
    let table_load_start = Instant::now();
    hg_engine.load_table("enterprise_survey", fragments)?;
    let table_load_time = table_load_start.elapsed();
    println!(" Table loaded into hypergraph in {:?}", table_load_time);
    
    // Connect to PostgreSQL
    println!("\n Connecting to PostgreSQL...");
    let username = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());
    let conn_strings = vec![
        format!("host=localhost user={} dbname=postgres", username),
        format!("host=localhost user={} dbname=postgres port=5432", username),
        "host=localhost user=postgres dbname=postgres".to_string(),
    ];
    
    let pg_client = {
        let mut client = None;
        for conn_str in &conn_strings {
            match tokio_postgres::connect(conn_str, NoTls).await {
                Ok((c, conn)) => {
                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            eprintln!("PostgreSQL connection error: {}", e);
                        }
                    });
                    client = Some(c);
                    break;
                }
                Err(_) => continue,
            }
        }
        client.ok_or("Could not connect to PostgreSQL")?
    };
    
    println!(" Connected to PostgreSQL");
    
    // Load data into PostgreSQL
    println!(" Loading data into PostgreSQL...");
    let pg_load_start = Instant::now();
    load_into_postgres(&pg_client, &columns, &csv_rows).await?;
    let pg_load_time = pg_load_start.elapsed();
    println!(" Data loaded into PostgreSQL in {:?}", pg_load_time);
    
    // Connect to DuckDB
    println!("\n Connecting to DuckDB...");
    let duckdb_conn = Connection::open_in_memory()?;
    println!(" Connected to DuckDB");
    
    // Load data into DuckDB
    println!(" Loading data into DuckDB...");
    let duckdb_load_start = Instant::now();
    load_into_duckdb(&duckdb_conn, &columns, &csv_rows)?;
    let duckdb_load_time = duckdb_load_start.elapsed();
    println!(" Data loaded into DuckDB in {:?}", duckdb_load_time);
    
    println!("\n{}", "=".repeat(80));
    println!(" Running Fair Benchmark Comparison");
    println!("{}", "=".repeat(80));
    println!("Each query runs 5 times (with 2 warmup runs) and we report the average");
    println!("Result cache is disabled for Hypergraph during benchmarking");
    println!("Comparing: Hypergraph SQL Engine vs PostgreSQL vs DuckDB");
    println!("{}", "=".repeat(80));
    
    const WARMUP_RUNS: usize = 2;
    const BENCHMARK_RUNS: usize = 5;
    
    let mut results = Vec::new();
    
    for (i, query) in queries.iter().enumerate() {
        println!("\n{} Query {}: {}", "".repeat(40), i + 1, query);
        println!("{}", "".repeat(80));
        
        // Hypergraph Engine Benchmark
        println!(" Hypergraph Engine:");
        let mut hg_times = Vec::new();
        let mut hg_rows = 0;
        
        // Clear ALL caches first for fair comparison
        hg_engine.clear_all_caches();
        
        // Warmup
        for _ in 0..WARMUP_RUNS {
            let _ = hg_engine.execute_query(query);
        }
        
        // Clear ALL caches again after warmup for fair comparison
        hg_engine.clear_all_caches();
        
        // Benchmark runs
        let mut hg_result_data: Option<hypergraph_sql_engine::execution::engine::QueryResult> = None;
        for run in 0..BENCHMARK_RUNS {
            match hg_engine.execute_query_with_cache(query, false) {
                Ok(result) => {
                    hg_times.push(result.execution_time_ms);
                    hg_rows = result.row_count;
                    if run == 0 {
                        print!("  Run 1: {:.5}ms", result.execution_time_ms);
                        // Clone the result to keep it for display
                        hg_result_data = Some(hypergraph_sql_engine::execution::engine::QueryResult {
                            batches: result.batches.clone(),
                            row_count: result.row_count,
                            execution_time_ms: result.execution_time_ms,
                        });
                    }
                }
                Err(e) => {
                    eprintln!("   Error: {}", e);
                    break;
                }
            }
        }
        
        let hg_avg = if hg_times.is_empty() {
            0.0
        } else {
            hg_times.iter().sum::<f64>() / hg_times.len() as f64
        };
        
        if hg_times.len() > 1 {
            println!(" ... (avg of {} runs: {:.5}ms)", hg_times.len(), hg_avg);
        }
        println!("   Average: {:.5}ms | Rows: {}", hg_avg, hg_rows);
        
        // Display Hypergraph results
        if let Some(ref result) = hg_result_data {
            display_hypergraph_results(&result.batches, 10);
        }
        
        // PostgreSQL Benchmark
        println!("\n PostgreSQL:");
        let mut pg_times = Vec::new();
        let mut pg_rows = 0;
        
        // Warmup
        for _ in 0..WARMUP_RUNS {
            let _ = pg_client.query(query, &[]).await;
        }
        
        // Benchmark runs
        let mut pg_result_rows: Option<Vec<tokio_postgres::Row>> = None;
        for run in 0..BENCHMARK_RUNS {
            let start = Instant::now();
            match pg_client.query(query, &[]).await {
                Ok(rows) => {
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    pg_times.push(elapsed);
                    pg_rows = rows.len();
                    if run == 0 {
                        print!("  Run 1: {:.5}ms", elapsed);
                        pg_result_rows = Some(rows);
                    }
                }
                Err(e) => {
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    eprintln!("   Error: {} (took {:.5}ms)", e, elapsed);
                    break;
                }
            }
        }
        
        let pg_avg = if pg_times.is_empty() {
            0.0
        } else {
            pg_times.iter().sum::<f64>() / pg_times.len() as f64
        };
        
        if pg_times.len() > 1 {
            println!(" ... (avg of {} runs: {:.5}ms)", pg_times.len(), pg_avg);
        }
        println!("   Average: {:.5}ms | Rows: {}", pg_avg, pg_rows);
        
        // Display PostgreSQL results
        if let Some(ref rows) = pg_result_rows {
            display_postgres_results(rows, 10);
        }
        
        // DuckDB Benchmark
        println!("\n DuckDB:");
        let mut duckdb_times = Vec::new();
        let mut duckdb_rows = 0;
        
        // Warmup
        for _ in 0..WARMUP_RUNS {
            let _ = duckdb_conn.execute(query, []);
        }
        
        // Benchmark runs
        let mut duckdb_result_data: Option<Vec<Vec<String>>> = None;
        for run in 0..BENCHMARK_RUNS {
            let start = Instant::now();
            match duckdb_conn.prepare(query) {
                Ok(mut stmt) => {
                    match stmt.query([]) {
                        Ok(mut rows) => {
                            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                            duckdb_times.push(elapsed);
                            if run == 0 {
                                print!("  Run 1: {:.5}ms", elapsed);
                                // Collect rows for display
                                let mut result_data = Vec::new();
                                let mut row_count = 0;
                                while let Ok(Some(row)) = rows.next() {
                                    let mut row_data = Vec::new();
                                    // Try to get values (limit to 20 columns)
                                    for col_idx in 0..20 {
                                        if let Ok(val) = row.get::<_, String>(col_idx) {
                                            row_data.push(val);
                                        } else if let Ok(val) = row.get::<_, i64>(col_idx) {
                                            row_data.push(val.to_string());
                                        } else if let Ok(val) = row.get::<_, f64>(col_idx) {
                                            row_data.push(val.to_string());
                                        } else {
                                            break;
                                        }
                                    }
                                    if !row_data.is_empty() {
                                        result_data.push(row_data);
                                        row_count += 1;
                                    }
                                }
                                duckdb_rows = row_count;
                                duckdb_result_data = Some(result_data);
                            } else {
                                // Count rows for other runs
                                let mut row_count = 0;
                                while let Ok(Some(_)) = rows.next() {
                                    row_count += 1;
                                }
                                duckdb_rows = row_count;
                            }
                        }
                        Err(e) => {
                            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                            eprintln!("   Error: {} (took {:.5}ms)", e, elapsed);
                            break;
                        }
                    }
                }
                Err(e) => {
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    eprintln!("   Error: {} (took {:.5}ms)", e, elapsed);
                    break;
                }
            }
        }
        
        let duckdb_avg = if duckdb_times.is_empty() {
            0.0
        } else {
            duckdb_times.iter().sum::<f64>() / duckdb_times.len() as f64
        };
        
        if duckdb_times.len() > 1 {
            println!(" ... (avg of {} runs: {:.5}ms)", duckdb_times.len(), duckdb_avg);
        }
        println!("   Average: {:.5}ms | Rows: {}", duckdb_avg, duckdb_rows);
        
        // Display DuckDB results
        if let Some(ref rows) = duckdb_result_data {
            display_duckdb_results(rows, 10);
        }
        
        // Comparison
        println!("\n Comparison:");
        let speedup_vs_pg = if pg_avg > 0.0 && hg_avg > 0.0 {
            pg_avg / hg_avg
        } else if pg_avg > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        
        let speedup_vs_duckdb = if duckdb_avg > 0.0 && hg_avg > 0.0 {
            duckdb_avg / hg_avg
        } else if duckdb_avg > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        
        if speedup_vs_pg > 1.0 {
            println!("   Hypergraph Engine is {:.5}x FASTER than PostgreSQL", speedup_vs_pg);
        } else if speedup_vs_pg < 1.0 && speedup_vs_pg > 0.0 {
            println!("   PostgreSQL is {:.5}x FASTER than Hypergraph", 1.0 / speedup_vs_pg);
        }
        
        if speedup_vs_duckdb > 1.0 {
            println!("   Hypergraph Engine is {:.5}x FASTER than DuckDB", speedup_vs_duckdb);
        } else if speedup_vs_duckdb < 1.0 && speedup_vs_duckdb > 0.0 {
            println!("   DuckDB is {:.5}x FASTER than Hypergraph", 1.0 / speedup_vs_duckdb);
        }
        
        results.push((query.clone(), hg_avg, pg_avg, duckdb_avg, speedup_vs_pg, speedup_vs_duckdb, hg_rows, pg_rows, duckdb_rows));
    }
    
    // Final Summary
    println!("\n{}", "=".repeat(80));
    println!(" FINAL SUMMARY");
    println!("{}", "=".repeat(80));
        println!("{:<50} {:>12} {:>12} {:>12} {:>12} {:>12}", "Query", "Hypergraph", "PostgreSQL", "DuckDB", "vs PG", "vs DuckDB");
        println!("{}", "".repeat(110));
        
        let mut total_speedup_pg = 0.0;
        let mut total_speedup_duckdb = 0.0;
        let mut valid_pg = 0;
        let mut valid_duckdb = 0;
        
        for (query, hg_time, pg_time, duckdb_time, speedup_pg, speedup_duckdb, _hg_rows, _pg_rows, _duckdb_rows) in &results {
            let query_short = if query.len() > 45 {
                format!("{}...", &query[..42])
            } else {
                query.clone()
            };
            
            let speedup_pg_str = if *speedup_pg == f64::INFINITY {
                "".to_string()
            } else if *speedup_pg > 0.0 {
                format!("{:.2}x", speedup_pg)
            } else {
                "N/A".to_string()
            };
            
            let speedup_duckdb_str = if *speedup_duckdb == f64::INFINITY {
                "".to_string()
            } else if *speedup_duckdb > 0.0 {
                format!("{:.2}x", speedup_duckdb)
            } else {
                "N/A".to_string()
            };
            
            println!("{:<50} {:>10.5}ms {:>10.5}ms {:>10.5}ms {:>12} {:>12}", 
                query_short, hg_time, pg_time, duckdb_time, speedup_pg_str, speedup_duckdb_str);
        
            if *speedup_pg > 0.0 && *speedup_pg != f64::INFINITY {
                total_speedup_pg += speedup_pg;
                valid_pg += 1;
            }
            
            if *speedup_duckdb > 0.0 && *speedup_duckdb != f64::INFINITY {
                total_speedup_duckdb += speedup_duckdb;
                valid_duckdb += 1;
            }
    }
    
        println!("{}", "".repeat(110));
        
        if valid_pg > 0 {
            let avg_speedup_pg = total_speedup_pg / valid_pg as f64;
            println!("\n Average Speedup vs PostgreSQL: {:.5}x", avg_speedup_pg);
        }
        
        if valid_duckdb > 0 {
            let avg_speedup_duckdb = total_speedup_duckdb / valid_duckdb as f64;
            println!(" Average Speedup vs DuckDB: {:.5}x", avg_speedup_duckdb);
            
            if avg_speedup_duckdb > 1.0 {
                println!(" WINNER: Hypergraph SQL Engine is {:.5}x faster than DuckDB on average!", avg_speedup_duckdb);
            } else if avg_speedup_duckdb < 1.0 {
                println!(" WINNER: DuckDB is {:.5}x faster than Hypergraph on average!", 1.0 / avg_speedup_duckdb);
            } else {
                println!("  Hypergraph and DuckDB performed similarly!");
            }
        }
    
    Ok(())
}

fn load_into_duckdb(
    conn: &Connection,
    columns: &[String],
    rows: &Vec<csv::StringRecord>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Drop table if exists
    let _ = conn.execute("DROP TABLE IF EXISTS enterprise_survey", []);
    
    // Create table - use TEXT for all columns for simplicity
    let col_defs: Vec<String> = columns.iter()
        .map(|c| format!("\"{}\" TEXT", c.replace(" ", "_").replace("-", "_").replace(".", "_")))
        .collect();
    let create_sql = format!("CREATE TABLE enterprise_survey ({})", col_defs.join(", "));
    conn.execute(&create_sql, [])?;
    
    // Insert data in batches
    let col_names: Vec<String> = columns.iter()
        .map(|c| format!("\"{}\"", c.replace(" ", "_").replace("-", "_").replace(".", "_")))
        .collect();
    
    let batch_size = 1000;
    for batch in rows.chunks(batch_size) {
        for row in batch {
            let params: Vec<String> = row.iter().map(|s| s.to_string()).collect();
            let values: Vec<String> = params.iter()
                .map(|s| format!("'{}'", s.replace("'", "''")))
                .collect();
            let insert_with_values = format!(
                "INSERT INTO enterprise_survey ({}) VALUES ({})",
                col_names.join(", "),
                values.join(", ")
            );
            conn.execute(&insert_with_values, [])?;
        }
    }
    Ok(())
}

fn load_csv(path: &str) -> Result<(usize, Vec<String>, Vec<(String, ColumnFragment)>, Vec<csv::StringRecord>), Box<dyn std::error::Error + Send + Sync>> {
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
    
    Ok((rows.len(), column_names, column_fragments, rows))
}

async fn load_into_postgres(
    client: &tokio_postgres::Client,
    columns: &[String],
    rows: &Vec<csv::StringRecord>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Drop table if exists
    let _ = client.execute("DROP TABLE IF EXISTS enterprise_survey", &[]).await;
    
    // Create table - use TEXT for all columns for simplicity
    let col_defs: Vec<String> = columns.iter()
        .map(|c| format!("\"{}\" TEXT", c.replace(" ", "_").replace("-", "_").replace(".", "_")))
        .collect();
    let create_sql = format!("CREATE TABLE enterprise_survey ({})", col_defs.join(", "));
    client.execute(&create_sql, &[]).await?;
    
    // Insert data in batches
    let col_names: Vec<String> = columns.iter()
        .map(|c| format!("\"{}\"", c.replace(" ", "_").replace("-", "_").replace(".", "_")))
        .collect();
    
    let batch_size = 1000;
    for batch in rows.chunks(batch_size) {
        for row in batch {
            let params: Vec<String> = row.iter().map(|s| s.to_string()).collect();
            let values: Vec<String> = params.iter()
                .map(|s| format!("'{}'", s.replace("'", "''")))
                .collect();
            let insert_with_values = format!(
                "INSERT INTO enterprise_survey ({}) VALUES ({})",
                col_names.join(", "),
                values.join(", ")
            );
            client.execute(&insert_with_values, &[]).await?;
        }
    }
    Ok(())
}


fn display_hypergraph_results(batches: &[hypergraph_sql_engine::execution::batch::ExecutionBatch], max_rows: usize) {
    use arrow::array::*;
    use arrow::datatypes::*;
    
    if batches.is_empty() {
        println!("  (No results)");
        return;
    }
    
    println!("\n   Hypergraph Results (showing first {} rows):", max_rows);
    
    // Get schema from first batch
    let schema = &batches[0].batch.schema;
    let num_cols = schema.fields().len();
    
    // Print header
    let header: Vec<String> = schema.fields().iter()
        .map(|f| f.name().clone())
        .collect();
    println!("  {}", "".repeat(80));
    print!("  ");
    for (idx, col_name) in header.iter().enumerate() {
        let display_name = if col_name.len() > 12 {
            format!("{}...", &col_name[..9])
        } else {
            col_name.clone()
        };
        print!("{:>12}", display_name);
        if idx < num_cols - 1 {
            print!(" |");
        }
    }
    println!();
    println!("  {}", "".repeat(80));
    
    // Print rows
    let mut rows_printed = 0;
    for batch in batches {
        if rows_printed >= max_rows {
            break;
        }
        
        let actual_row_count = batch.batch.columns.first()
            .map(|col| col.len())
            .unwrap_or(0)
            .min(batch.row_count)
            .min(batch.selection.len());
        
        for row_idx in 0..actual_row_count.min(max_rows - rows_printed) {
            if row_idx < batch.selection.len() && !batch.selection[row_idx] {
                continue;
            }
            
            print!("  ");
            for col_idx in 0..num_cols {
                if let Some(col_array) = batch.batch.column(col_idx) {
                    let value = match col_array.data_type() {
                        DataType::Int64 => {
                            let arr = col_array.as_any().downcast_ref::<Int64Array>().unwrap();
                            if arr.is_null(row_idx) {
                                "NULL".to_string()
                            } else {
                                arr.value(row_idx).to_string()
                            }
                        }
                        DataType::Float64 => {
                            let arr = col_array.as_any().downcast_ref::<Float64Array>().unwrap();
                            if arr.is_null(row_idx) {
                                "NULL".to_string()
                            } else {
                                arr.value(row_idx).to_string()
                            }
                        }
                        DataType::Utf8 => {
                            let arr = col_array.as_any().downcast_ref::<StringArray>().unwrap();
                            if arr.is_null(row_idx) {
                                "NULL".to_string()
                            } else {
                                let val = arr.value(row_idx);
                                if val.len() > 12 {
                                    format!("{}...", &val[..9])
                                } else {
                                    val.to_string()
                                }
                            }
                        }
                        _ => "?".to_string(),
                    };
                    print!("{:>12}", value);
                } else {
                    print!("{:>12}", "?");
                }
                if col_idx < num_cols - 1 {
                    print!(" |");
                }
            }
            println!();
            rows_printed += 1;
        }
        
        if rows_printed >= max_rows {
            break;
        }
    }
    
    if rows_printed >= max_rows {
        println!("  ... (showing first {} rows)", max_rows);
    }
    println!();
}

fn display_postgres_results(rows: &[tokio_postgres::Row], max_rows: usize) {
    if rows.is_empty() {
        println!("  (No results)");
        return;
    }
    
    println!("\n   PostgreSQL Results (showing first {} rows):", max_rows);
    
    // Get column count from first row
    let num_cols = rows[0].len();
    
    // Print header
    println!("  {}", "".repeat(80));
    print!("  ");
    for col_idx in 0..num_cols {
        let col_name = format!("col_{}", col_idx);
        print!("{:>12}", if col_name.len() > 12 { format!("{}...", &col_name[..9]) } else { col_name });
        if col_idx < num_cols - 1 {
            print!(" |");
        }
    }
    println!();
    println!("  {}", "".repeat(80));
    
    // Print rows
    for (idx, row) in rows.iter().take(max_rows).enumerate() {
        print!("  ");
        for col_idx in 0..num_cols {
            let value: String = match row.try_get::<_, String>(col_idx) {
                Ok(v) => {
                    if v.len() > 12 {
                        format!("{}...", &v[..9])
                    } else {
                        v
                    }
                }
                Err(_) => {
                    if let Ok(v) = row.try_get::<_, i64>(col_idx) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, f64>(col_idx) {
                        v.to_string()
                    } else {
                        "NULL".to_string()
                    }
                }
            };
            print!("{:>12}", value);
            if col_idx < num_cols - 1 {
                print!(" |");
            }
        }
        println!();
    }
    
    if rows.len() > max_rows {
        println!("  ... (showing first {} of {} rows)", max_rows, rows.len());
    }
    println!();
}

fn display_duckdb_results(rows: &[Vec<String>], max_rows: usize) {
    if rows.is_empty() {
        println!("  (No results)");
        return;
    }
    
    println!("\n   DuckDB Results (showing first {} rows):", max_rows);
    
    // Get column count from first row
    let num_cols = rows[0].len();
    
    // Print header
    println!("  {}", "".repeat(80));
    print!("  ");
    for col_idx in 0..num_cols {
        let col_name = format!("col_{}", col_idx);
        print!("{:>12}", if col_name.len() > 12 { format!("{}...", &col_name[..9]) } else { col_name });
        if col_idx < num_cols - 1 {
            print!(" |");
        }
    }
    println!();
    println!("  {}", "".repeat(80));
    
    // Print rows
    for row in rows.iter().take(max_rows) {
        print!("  ");
        for (col_idx, value) in row.iter().enumerate() {
            let display_value = if value.len() > 12 {
                format!("{}...", &value[..9])
            } else {
                value.clone()
            };
            print!("{:>12}", display_value);
            if col_idx < num_cols - 1 {
                print!(" |");
            }
        }
        println!();
    }
    
    if rows.len() > max_rows {
        println!("  ... (showing first {} of {} rows)", max_rows, rows.len());
    }
    println!();
}
