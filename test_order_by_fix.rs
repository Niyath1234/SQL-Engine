use hypergraph_sql_engine::engine::HypergraphSQLEngine;
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
        
        let fragment = hypergraph_sql_engine::storage::fragment::ColumnFragment::new(
            array,
            hypergraph_sql_engine::storage::fragment::FragmentMetadata {
                row_count: rows.len(),
                min_value: None,
                max_value: None,
                cardinality: rows.len(),
                compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
                memory_size: 0,
            }
        );
        
        fragments.push((col_name.clone(), fragment));
    }
    
    Ok((rows.len(), headers, fragments))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = HypergraphSQLEngine::new();
    
    // Load CSV
    let csv_path = "annual-enterprise-survey-2024-financial-year-provisional.csv";
    println!("Loading CSV: {}", csv_path);
    let (row_count, headers, fragments) = load_csv(csv_path)?;
    println!("Loaded {} rows with {} columns", row_count, headers.len());
    
    // Load table
    println!("Loading table 'adh'...");
    engine.load_table("adh", fragments)?;
    println!("Table loaded successfully!");
    
    // Test query
    let query = "SELECT Year, SUM(Value) as total_value, AVG(Value) as avg_value, MIN(Value) as min_value, MAX(Value) as max_value FROM adh WHERE Value > 1000 AND Year >= 2020 GROUP BY Year HAVING SUM(Value) > 50000 ORDER BY total_value DESC LIMIT 10";
    
    println!("\n Testing query:");
    println!("{}", query);
    println!();
    
    match engine.execute_query(query) {
        Ok(result) => {
            println!(" SUCCESS! {} rows returned in {:.5}ms", result.row_count, result.execution_time_ms);
            if !result.batches.is_empty() {
                let batch = &result.batches[0];
                let schema = &batch.batch.schema;
                println!("Columns: {:?}", schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            }
        }
        Err(e) => {
            println!(" ERROR: {}", e);
            println!("\nFull error details:");
            println!("{:?}", e);
        }
    }
    
    Ok(())
}

