use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use std::io::{self, Write};
use std::fs::File;
use csv::ReaderBuilder;
use arrow::array::*;
use std::sync::Arc;

fn load_csv(path: &str) -> anyhow::Result<(usize, Vec<String>, Vec<(String, hypergraph_sql_engine::storage::fragment::ColumnFragment)>)> {
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
        
        let row_count = array.len();
        let fragment = hypergraph_sql_engine::storage::fragment::ColumnFragment::new(
            array,
            hypergraph_sql_engine::storage::fragment::FragmentMetadata {
                row_count,
                min_value: None,
                max_value: None,
                cardinality: row_count,
                compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
                memory_size: 0,
                table_name: None,
                column_name: None,
            }
        );
        
        fragments.push((col_name.clone(), fragment));
    }
    
    Ok((rows.len(), headers, fragments))
}

fn print_query_result(result: &hypergraph_sql_engine::execution::engine::QueryResult, max_rows: usize) {
    // Check if result was cached (execution_time_ms == 0.0 typically indicates cache hit)
    let cache_indicator = if result.execution_time_ms == 0.0 && result.row_count > 0 {
        " (cached)"
    } else {
        ""
    };
    println!("\n Result: {} rows returned in {:.5}ms{}", result.row_count, result.execution_time_ms, cache_indicator);
    
    if !result.batches.is_empty() {
        eprintln!(" DEBUG: print_query_result - {} batches available", result.batches.len());
        let batch = &result.batches[0];
        
        // Check schema directly from batch
        eprintln!(" DEBUG: Using batch[0] - row_count={}", batch.row_count);
        eprintln!(" DEBUG: Batch schema field count: {}", batch.batch.schema.fields().len());
        eprintln!(" DEBUG: Batch schema field names: {:?}", 
            batch.batch.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        eprintln!(" DEBUG: Batch has {} columns", batch.batch.columns.len());
        
        // Check each column
        for (i, col) in batch.batch.columns.iter().enumerate() {
            eprintln!(" DEBUG: Column[{}]: type={}, len={}, name={:?}", 
                i, col.data_type(), col.len(), 
                if i < batch.batch.schema.fields().len() {
                    Some(batch.batch.schema.field(i).name())
                } else {
                    None
                });
        }
        
        let schema = &batch.batch.schema;
        let num_cols = schema.fields().len();
        
        // CRITICAL: Check for duplicate field names in schema and deduplicate for display
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
        let unique_names: std::collections::HashSet<String> = field_names.iter().cloned().collect();
        
        // Build a deduplicated list of field indices for display
        // When duplicates exist, prefer the one with the correct type (Int64 for COUNT, Float64 for SUM/AVG)
        let mut display_indices = vec![];
        let mut seen_names: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name().to_string();
            if let Some(&existing_idx) = seen_names.get(&field_name) {
                // Duplicate found - check which one to keep
                let existing_field = schema.field(existing_idx);
                let existing_type = existing_field.data_type();
                let current_type = field.data_type();
                
                // Prefer Int64 for COUNT aggregates, Float64 for SUM/AVG
                let should_replace = match (existing_type, current_type) {
                    (arrow::datatypes::DataType::Float64, arrow::datatypes::DataType::Int64) => {
                        // Prefer Int64 over Float64 (likely COUNT vs SUM/AVG)
                        true
                    }
                    (arrow::datatypes::DataType::Int64, arrow::datatypes::DataType::Float64) => {
                        // Keep Int64, don't replace with Float64
                        false
                    }
                    _ => {
                        // Same type or other - keep first occurrence
                        false
                    }
                };
                
                if should_replace {
                    // Replace the existing index with the current one
                    seen_names.insert(field_name, i);
                    // Find and replace in display_indices
                    if let Some(pos) = display_indices.iter().position(|&x| x == existing_idx) {
                        display_indices[pos] = i;
                    }
                }
            } else {
                display_indices.push(i);
                seen_names.insert(field_name, i);
            }
        }
        
        if field_names.len() != unique_names.len() {
            eprintln!(" ERROR: Display batch schema has duplicate field names: {:?}", field_names);
            eprintln!("  WARNING: Using {} unique columns for display (removed {} duplicates)", 
                display_indices.len(), field_names.len() - display_indices.len());
        }
        
        // Verify column count matches schema field count
        if batch.batch.columns.len() != num_cols {
            eprintln!("  WARNING: Column count ({}) != schema field count ({})", 
                batch.batch.columns.len(), num_cols);
        }
        
        // Use deduplicated count for display
        let display_num_cols = display_indices.len();
        if display_num_cols == 0 || batch.row_count == 0 {
            println!();
            return;
        }
        
        // Print header using deduplicated indices
        print!("\n");
        for (idx, &i) in display_indices.iter().enumerate() {
            let field = schema.field(i);
            let col_name = if field.name().len() > 20 {
                &field.name()[..20]
            } else {
                field.name()
            };
            print!("{:>20}", col_name);
            if idx < display_num_cols - 1 {
                print!(" |");
            }
        }
        println!();
        println!("{}", "-".repeat(display_num_cols * 23));
        
        // Print rows using deduplicated indices
        let rows_to_show = max_rows.min(batch.row_count);
        let mut printed = 0;
        for row_idx in 0..batch.row_count {
            if printed >= rows_to_show {
                break;
            }
            if !batch.selection[row_idx] {
                continue;
            }
            for (idx, &col_idx) in display_indices.iter().enumerate() {
                if let Some(array) = batch.batch.column(col_idx) {
                    let value = if array.is_null(row_idx) {
                        "NULL".to_string()
                    } else {
                        match array.data_type() {
                            arrow::datatypes::DataType::Int64 => {
                                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                // Handle Option<i64> arrays - check if we have Option<> or direct values
                                if row_idx < arr.len() {
                                    if arr.is_null(row_idx) {
                                        "NULL".to_string()
                                    } else {
                                        arr.value(row_idx).to_string()
                                    }
                                } else {
                                    "N/A".to_string()
                                }
                            }
                            arrow::datatypes::DataType::Float64 => {
                                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                // Handle Option<f64> arrays
                                if row_idx < arr.len() {
                                    if arr.is_null(row_idx) {
                                        "NULL".to_string()
                                    } else {
                                        format!("{:.2}", arr.value(row_idx))
                                    }
                                } else {
                                    "N/A".to_string()
                                }
                            }
                            arrow::datatypes::DataType::Utf8 => {
                                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                                if row_idx < arr.len() {
                                    if arr.is_null(row_idx) {
                                        "NULL".to_string()
                                    } else {
                                        let val = arr.value(row_idx);
                                        if val.len() > 20 {
                                            format!("{}...", &val[..17])
                                        } else {
                                            val.to_string()
                                        }
                                    }
                                } else {
                                    "N/A".to_string()
                                }
                            }
                            _ => "?".to_string(),
                        }
                    };
                    print!("{:>20}", value);
                    if idx < display_num_cols - 1 {
                        print!(" |");
                    }
                }
            }
            println!();
            printed += 1;
        }
        
        if batch.row_count > rows_to_show {
            println!("\n... (showing first {} of {} rows)", rows_to_show, batch.row_count);
        }
    }
    println!();
}

fn main() -> anyhow::Result<()> {
    println!(" Hypergraph SQL Engine - Interactive Query Tool");
    println!("{}", "=".repeat(80));
    println!();
    
    // Try to load CSV data
    let csv_path = "annual-enterprise-survey-2024-financial-year-provisional.csv";
    let mut engine = HypergraphSQLEngine::new();
    
    if std::path::Path::new(csv_path).exists() {
        println!(" Loading data from: {}", csv_path);
        match load_csv(csv_path) {
            Ok((row_count, columns, fragments)) => {
                engine.load_table("adh", fragments)?;
                println!(" Loaded {} rows, {} columns into table 'adh'", row_count, columns.len());
            }
            Err(e) => {
                println!("  Failed to load CSV: {}", e);
                println!("   You can still run queries, but tables need to be loaded first.");
            }
        }
    } else {
        println!("  CSV file not found: {}", csv_path);
        println!("   You can still run queries, but tables need to be loaded first.");
    }
    
    println!();
    println!(" Type SQL queries (or 'exit' to quit, 'help' for commands)");
    println!("    Multi-line queries: Type each line, end with ';' to execute");
    println!("    Commands: 'show' to view current query, 'cancel' to clear buffer");
    println!("{}", "=".repeat(80));
    println!();
    
    let stdin = io::stdin();
    let mut query_buffer = String::new();
    let mut line_number = 1;
    
    loop {
        // Show appropriate prompt based on whether we're in multi-line mode
        if query_buffer.is_empty() {
            print!("sql> ");
        } else {
            print!("sql[{}]> ", line_number);
        }
        io::stdout().flush()?;
        
        let mut line = String::new();
        stdin.read_line(&mut line)?;
        
        let line = line.trim();
        
        // Handle commands
        if line.is_empty() {
            // Empty line in multi-line mode: continue to next line
            if !query_buffer.is_empty() {
                query_buffer.push('\n');
                line_number += 1;
            }
            continue;
        }
        
        if line == "exit" || line == "quit" || line == "q" {
            if !query_buffer.is_empty() {
                println!("  You have an unsaved query. Type 'cancel' to discard it, or 'exit' again to quit anyway.");
                query_buffer.clear();
                line_number = 1;
                continue;
            }
            println!(" Goodbye!");
            break;
        }
        
        if line == "help" || line == "?" {
            println!("\n Available commands:");
            println!("   exit, quit, q     - Exit the REPL");
            println!("   help, ?           - Show this help");
            println!("   clear             - Clear the screen");
            println!("   clear cache       - Clear query result cache");
            println!("   describe <table>  - Show table schema");
            println!("   show              - Show current multi-line query buffer");
            println!("   cancel            - Cancel/clear current multi-line query");
            println!("\n Multi-line Query Tips:");
            println!("   - Type your SQL query line by line");
            println!("   - End with ';' to execute");
            println!("   - Press Enter on empty line to add new line");
            println!("   - Use 'cancel' to discard current query");
            println!("   - Use 'show' to view what you've typed so far");
            println!("\n SQL Features:");
            println!("   - SELECT with WHERE, JOIN, GROUP BY, ORDER BY, LIMIT");
            println!("   - Window Functions (ROW_NUMBER, RANK, SUM OVER)");
            println!("   - CTEs (WITH ... AS ...)");
            println!("   - Aggregations (COUNT, SUM, AVG, MIN, MAX)");
            println!("   - Vector Search (VECTOR_SIMILARITY, VECTOR_DISTANCE)");
            println!();
            continue;
        }
        
        if line == "clear" {
            print!("\x1B[2J\x1B[1;1H");
            continue;
        }
        
        if line == "clear cache" || line == "clearcache" {
            engine.clear_result_cache();
            println!("\n Query result cache cleared!\n");
            continue;
        }
        
        // Show current query buffer
        if line == "show" || line == "view" {
            if query_buffer.is_empty() {
                println!("\n No query in buffer. Start typing your query.\n");
            } else {
                println!("\n Current query buffer ({} lines):", line_number);
                println!("{}", "-".repeat(80));
                for (i, query_line) in query_buffer.lines().enumerate() {
                    println!("{:3} | {}", i + 1, query_line);
                }
                println!("{}", "-".repeat(80));
                println!();
            }
            continue;
        }
        
        // Cancel/clear current query
        if line == "cancel" || line == "clear query" || line == "reset" {
            if query_buffer.is_empty() {
                println!("\n No query to cancel.\n");
            } else {
                println!("\n Query cancelled. Buffer cleared.\n");
                query_buffer.clear();
                line_number = 1;
            }
            continue;
        }
        
        // Handle DESCRIBE command
        if line.to_lowercase().starts_with("describe ") {
            let table_name = line.split_whitespace().nth(1).unwrap_or("");
            if table_name.is_empty() {
                println!("\n Error: DESCRIBE requires a table name\n");
                continue;
            }
            match engine.describe_table(table_name) {
                Ok(schema) => {
                    println!("\n Table: {}", table_name);
                    println!("{0:<30}{0:<20}", "");
                    println!(" {:<28}  {:<18} ", "Column", "Type");
                    println!("{0:<30}{0:<20}", "");
                    for (col_name, col_type) in &schema {
                        // Clean up type name (remove "DataType::" prefix)
                        let clean_type = col_type.replace("DataType::", "").replace("Utf8", "VARCHAR");
                        println!(" {:<28}  {:<18} ", col_name, clean_type);
                    }
                    println!("{0:<30}{0:<20}", "");
                    println!("\n{} columns\n", schema.len());
                }
                Err(e) => {
                    println!("\n Error: {}\n", e);
                }
            }
            continue;
        }
        
        // Check if line ends with semicolon (indicates end of query)
        let line_ends_query = line.ends_with(';');
        
        // Add line to buffer
        if !query_buffer.is_empty() {
            query_buffer.push('\n');
        }
        // Remove semicolon temporarily (we'll add it back if needed)
        let line_to_add = if line_ends_query {
            &line[..line.len()-1]
        } else {
            line
        };
        query_buffer.push_str(line_to_add);
        
        // If line ends with semicolon, execute the query
        if line_ends_query {
            let query_raw = query_buffer.trim().to_string();
            query_buffer.clear();
            line_number = 1;
            
            if query_raw.is_empty() {
                continue;
            }
            
            // Show the query being executed (for multi-line queries)
            if query_raw.contains('\n') {
                println!("\n Executing query:");
                println!("{}", "-".repeat(80));
                for (i, query_line) in query_raw.lines().enumerate() {
                    println!("{:3} | {}", i + 1, query_line);
                }
                println!("{}", "-".repeat(80));
            }
            
            // Normalize query: replace newlines with spaces, collapse multiple spaces
            // This ensures proper spacing when query spans multiple lines
            let mut normalized_query = query_raw.replace("\n", " ");
            // Collapse multiple spaces to single space
            while normalized_query.contains("  ") {
                normalized_query = normalized_query.replace("  ", " ");
            }
            let query = normalized_query.trim().to_string();
            
            // Execute query
            match engine.execute_query(&query) {
                Ok(result) => {
                    // Show all rows (use usize::MAX to remove limit)
                    print_query_result(&result, usize::MAX);
                }
                Err(e) => {
                    println!("\n Error: {}\n", e);
                    // Show query context for debugging
                    if query_raw.contains('\n') {
                        println!(" Query that failed:");
                        for (i, query_line) in query_raw.lines().enumerate() {
                            println!("   {:3} | {}", i + 1, query_line);
                        }
                        println!();
                    }
                }
            }
        } else {
            // Continue reading multi-line query
            line_number += 1;
        }
    }
    
    Ok(())
}

