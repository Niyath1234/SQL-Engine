/// Test script to demonstrate metadata storage in HyperNode
/// This loads the CSV table and shows what metadata is stored in nodes

use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use hypergraph_sql_engine::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType};
use csv;
use arrow::array::*;
use std::sync::Arc;

/// Load CSV file and create column fragments (matches server.rs function signature)
fn load_csv_file(path: &str) -> anyhow::Result<(usize, Vec<String>, Vec<(String, hypergraph_sql_engine::storage::fragment::ColumnFragment)>, Vec<csv::StringRecord>)> {
    let mut reader = csv::Reader::from_path(path)?;
    let headers = reader.headers()?.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut string_records = Vec::new();
    
    for result in reader.records() {
        let record = result?;
        string_records.push(record.clone());
        rows.push(record.iter().map(|s| s.to_string()).collect::<Vec<_>>());
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
            }
        );
        
        fragments.push((col_name.clone(), fragment));
    }
    
    Ok((rows.len(), headers, fragments, string_records))
}

fn main() -> anyhow::Result<()> {
    println!("=== METADATA STORAGE TEST ===\n");
    
    // Create engine
    let mut engine = HypergraphSQLEngine::default();
    
    // Load CSV file
    let csv_path = "/Users/niyathnair/Downloads/LQS/annual-enterprise-survey-2024-financial-year-provisional.csv";
    println!("Loading CSV file: {}", csv_path);
    
    // Load CSV into table
    let table_name = "annual_enterprise_survey";
    let (row_count, _column_names, columns, _rows) = load_csv_file(csv_path)?;
    
    println!("Loaded {} rows and {} columns from CSV\n", row_count, columns.len());
    
    // Load table into engine
    let table_node_id = engine.load_table(table_name, columns)?;
    println!("Table '{}' loaded with node_id: {:?}\n", table_name, table_node_id);
    
    // Get table node to inspect metadata using engine's graph() method
    let graph = engine.graph();
    let table_node = graph.get_table_node(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table not found"))?;
    
    println!("=== INITIAL NODE METADATA ===\n");
    print_node_metadata(&table_node);
    
    // Run some queries to trigger metadata collection
    println!("\n=== RUNNING QUERIES TO TRIGGER METADATA COLLECTION ===\n");
    
    // Query 1: Simple SELECT
    println!("Query 1: SELECT * FROM {} LIMIT 5", table_name);
    let result1 = engine.execute_query(&format!("SELECT * FROM {} LIMIT 5", table_name))?;
    println!("Result: {} rows\n", result1.row_count);
    
    // Query 2: SELECT with WHERE
    println!("Query 2: SELECT Year, Industry_name_NZSIOC FROM {} WHERE Year > 2020 LIMIT 10", table_name);
    let result2 = engine.execute_query(&format!(
        "SELECT Year, Industry_name_NZSIOC FROM {} WHERE Year > 2020 LIMIT 10", 
        table_name
    ))?;
    println!("Result: {} rows\n", result2.row_count);
    
    // Query 3: Aggregation
    println!("Query 3: SELECT COUNT(*) FROM {}", table_name);
    let result3 = engine.execute_query(&format!("SELECT COUNT(*) FROM {}", table_name))?;
    println!("Result: {} rows\n", result3.row_count);
    
    // Create a second table for JOIN testing
    println!("=== CREATING SECOND TABLE FOR JOIN TEST ===\n");
    let table2_name = "industry_stats";
    
    // First, get some sample industry codes from the main table to match on
    println!("Getting sample data from main table for JOIN...");
    let sample_query = format!("SELECT DISTINCT Industry_code_NZSIOC FROM {} LIMIT 5", table_name);
    let sample_result = engine.execute_query(&sample_query)?;
    println!("Found {} distinct industry codes\n", sample_result.row_count);
    
    // Create a simple industry statistics table that can join with the main table
    let create_table2_sql = format!(
        "CREATE TABLE {} (
            industry_code VARCHAR(50) PRIMARY KEY,
            industry_name VARCHAR(200) NOT NULL,
            total_enterprises INT,
            avg_value FLOAT
        )", table2_name
    );
    
    println!("Creating table: {}", table2_name);
    engine.execute_query(&create_table2_sql)?;
    println!("Table '{}' created successfully\n", table2_name);
    
    // Insert sample data for JOIN - use actual codes from CSV (simplified approach)
    // We'll insert codes that might match, or use a simpler join key like Industry_name_NZSIOC
    let insert_sql1 = format!(
        "INSERT INTO {} (industry_code, industry_name, total_enterprises, avg_value) VALUES
        ('Level 1', 'Agriculture, Forestry and Fishing', 1500, 125000.50),
        ('Level 1', 'Manufacturing', 3200, 450000.75),
        ('Level 1', 'Retail Trade', 2800, 320000.25),
        ('Level 1', 'Professional, Scientific and Technical Services', 1900, 580000.00),
        ('Level 1', 'Accommodation and Food Services', 1200, 210000.50)",
        table2_name
    );
    
    println!("Inserting data into '{}'", table2_name);
    engine.execute_query(&insert_sql1)?;
    println!("Data inserted successfully\n");
    
    // Query 4: Simple JOIN query to trigger join statistics
    println!("Query 4: Simple JOIN query between tables");
    let join_query = format!(
        "SELECT s.Year, i.industry_name 
         FROM {} s 
         JOIN {} i ON s.Industry_name_NZSIOC = i.industry_name 
         LIMIT 10",
        table_name, table2_name
    );
    println!("JOIN Query: {}", join_query);
    match engine.execute_query(&join_query) {
        Ok(result4) => {
            println!("Result: {} rows\n", result4.row_count);
        }
        Err(e) => {
            println!("JOIN Query failed: {}\n", e);
            // Continue anyway to show metadata
        }
    }
    
    // Query 5: Another JOIN to increase join frequency
    println!("Query 5: Another JOIN query");
    let join_query2 = format!(
        "SELECT COUNT(*) 
         FROM {} s
         JOIN {} i ON s.Industry_name_NZSIOC = i.industry_name",
        table_name, table2_name
    );
    println!("JOIN Query 2: {}", join_query2);
    match engine.execute_query(&join_query2) {
        Ok(result5) => {
            println!("Result: {} rows\n", result5.row_count);
        }
        Err(e) => {
            println!("JOIN Query 2 failed: {}\n", e);
        }
    }
    
    // Get updated table nodes for both tables
    let graph_updated = engine.graph();
    let updated_table_node = graph_updated.get_table_node(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table not found"))?;
    let updated_table2_node = graph_updated.get_table_node(table2_name)
        .ok_or_else(|| anyhow::anyhow!("Table 2 not found"))?;
    
    println!("=== UPDATED NODE METADATA (Table 1: {}) ===\n", table_name);
    print_node_metadata(&updated_table_node);
    
    println!("\n=== UPDATED NODE METADATA (Table 2: {}) ===\n", table2_name);
    print_node_metadata(&updated_table2_node);
    
    // Show detailed metadata breakdown
    println!("\n=== DETAILED METADATA BREAKDOWN (Table 1: {}) ===\n", table_name);
    show_detailed_metadata(&updated_table_node);
    
    println!("\n=== DETAILED METADATA BREAKDOWN (Table 2: {}) ===\n", table2_name);
    show_detailed_metadata(&updated_table2_node);
    
    Ok(())
}

fn print_node_metadata(node: &hypergraph_sql_engine::hypergraph::node::HyperNode) {
    println!("Node ID: {:?}", node.id);
    println!("Node Type: {:?}", node.node_type);
    println!("Table Name: {:?}", node.table_name);
    println!("Column Name: {:?}", node.column_name);
    println!("Stats: row_count={}, cardinality={}, size_bytes={}, last_updated={}", 
        node.stats.row_count, node.stats.cardinality, node.stats.size_bytes, node.stats.last_updated);
    println!("\nMetadata Keys ({}):", node.metadata.len());
    for (key, value) in &node.metadata {
        let display_value = if value.len() > 200 {
            format!("{}... (truncated)", &value[..200])
        } else {
            value.clone()
        };
        println!("  - {}: {}", key, display_value);
    }
}

fn show_detailed_metadata(node: &hypergraph_sql_engine::hypergraph::node::HyperNode) {
    // Parse and display structured metadata
    use serde_json::Value;
    
    // Column names
    if let Some(col_names_json) = node.metadata.get("column_names") {
        if let Ok(col_names) = serde_json::from_str::<Vec<String>>(col_names_json) {
            println!("Column Names ({}):", col_names.len());
            for (idx, name) in col_names.iter().enumerate() {
                println!("  {}. {}", idx + 1, name);
            }
            println!();
        }
    }
    
    // Column types
    if let Some(col_types_json) = node.metadata.get("column_types") {
        if let Ok(col_types) = serde_json::from_str::<Vec<String>>(col_types_json) {
            println!("Column Types:");
            for col_type in &col_types {
                println!("  - {}", col_type);
            }
            println!();
        }
    }
    
    // Access patterns
    if let Some(access_patterns_json) = node.metadata.get("access_patterns") {
        println!("Access Patterns:");
        println!("  Raw JSON: {}", if access_patterns_json.len() > 500 {
            format!("{}... (truncated)", &access_patterns_json[..500])
        } else {
            access_patterns_json.clone()
        });
        
        if let Ok(access_patterns) = serde_json::from_str::<Value>(access_patterns_json) {
            if let Some(freq) = access_patterns.get("access_frequency").and_then(|v| v.as_f64()) {
                println!("  - Access Frequency: {:.2} accesses/sec", freq);
            }
            if let Some(hotness) = access_patterns.get("hotness_score").and_then(|v| v.as_f64()) {
                println!("  - Hotness Score: {:.2} (0.0=cold, 1.0=hot)", hotness);
            }
            if let Some(cols) = access_patterns.get("frequently_accessed_columns").and_then(|v| v.as_array()) {
                println!("  - Frequently Accessed Columns ({}):", cols.len());
                for col in cols.iter().take(10) {
                    if let Some(col_name) = col.as_str() {
                        println!("    * {}", col_name);
                    }
                }
            }
        }
        println!();
    }
    
    // Column statistics
    if let Some(col_stats_json) = node.metadata.get("column_statistics") {
        println!("Column Statistics:");
        println!("  Raw JSON length: {} bytes", col_stats_json.len());
        if let Ok(col_stats) = serde_json::from_str::<Value>(col_stats_json) {
            if let Some(map) = col_stats.as_object() {
                println!("  Columns with stats: {}", map.len());
                for (col_name, stats) in map.iter().take(5) {
                    println!("    - {}: null_count={}, distinct_count={}", 
                        col_name,
                        stats.get("null_count").and_then(|v| v.as_u64()).unwrap_or(0),
                        stats.get("distinct_count").and_then(|v| v.as_u64()).unwrap_or(0)
                    );
                }
            }
        }
        println!();
    }
    
    // Index metadata
    if let Some(index_meta_json) = node.metadata.get("index_metadata") {
        println!("Index Metadata:");
        println!("  Raw JSON length: {} bytes", index_meta_json.len());
        if let Ok(index_meta) = serde_json::from_str::<Value>(index_meta_json) {
            if let Some(map) = index_meta.as_object() {
                println!("  Columns with indexes: {}", map.len());
                for (col_name, indexes) in map.iter().take(5) {
                    if let Some(indexes_array) = indexes.as_array() {
                        println!("    - {}: {} index(es)", col_name, indexes_array.len());
                    }
                }
            }
        }
        println!();
    }
    
    // Join statistics
    if let Some(join_stats_json) = node.metadata.get("join_statistics") {
        println!("Join Statistics:");
        println!("  Raw JSON length: {} bytes", join_stats_json.len());
        if let Ok(join_stats) = serde_json::from_str::<Value>(join_stats_json) {
            if let Some(partners) = join_stats.get("join_partners").and_then(|v| v.as_array()) {
                println!("  Join Partners: {}", partners.len());
                for partner in partners.iter().take(5) {
                    if let Some(table) = partner.get("table").and_then(|v| v.as_str()) {
                        let freq = partner.get("join_frequency").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let sel = partner.get("join_selectivity").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        println!("    - {}: frequency={:.2}, selectivity={:.2}", table, freq, sel);
                    }
                }
            }
        }
        println!();
    }
    
    // Optimization hints
    if let Some(opt_hints_json) = node.metadata.get("optimization_hints") {
        println!("Optimization Hints:");
        println!("  Raw JSON length: {} bytes", opt_hints_json.len());
        if let Ok(opt_hints) = serde_json::from_str::<Value>(opt_hints_json) {
            if let Some(strategy) = opt_hints.get("optimal_scan_strategy").and_then(|v| v.as_str()) {
                println!("  - Optimal Scan Strategy: {}", strategy);
            }
            if let Some(cache) = opt_hints.get("filter_selectivity_cache").and_then(|v| v.as_object()) {
                println!("  - Cached Filter Selectivities: {}", cache.len());
                for (filter, sel) in cache.iter().take(5) {
                    println!("    * {}: {:.2}", 
                        if filter.len() > 50 { &filter[..50] } else { filter },
                        sel.as_f64().unwrap_or(0.0)
                    );
                }
            }
        }
        println!();
    }
    
    // Table aliases
    if let Some(aliases_json) = node.metadata.get("table_aliases") {
        if let Ok(aliases) = serde_json::from_str::<Value>(aliases_json) {
            if let Some(aliases_map) = aliases.as_object() {
                if !aliases_map.is_empty() {
                    println!("Table Aliases ({}):", aliases_map.len());
                    for (alias, table) in aliases_map.iter().take(10) {
                        println!("  {} -> {}", alias, table.as_str().unwrap_or(""));
                    }
                    println!();
                }
            }
        }
    }
}

