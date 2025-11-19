use crate::engine::HypergraphSQLEngine;
use crate::storage::fragment::{ColumnFragment, FragmentMetadata, CompressionType};
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use csv::Reader;
use tokio_postgres;

pub fn load_csv_to_hypergraph(
    engine: &mut HypergraphSQLEngine,
    csv_path: &str,
    table_name: &str,
) -> Result<usize> {
    let mut reader = Reader::from_path(csv_path)?;
    let headers = reader.headers()?.clone();
    
    // Read all records
    let mut records: Vec<HashMap<String, String>> = vec![];
    for result in reader.records() {
        let record = result?;
        let mut row = HashMap::new();
        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                row.insert(header.to_string(), field.to_string());
            }
        }
        records.push(row);
    }
    
    if records.is_empty() {
        return Ok(0);
    }
    
    let row_count = records.len();
    
    // Build columns
    let mut columns: Vec<(String, ColumnFragment)> = vec![];
    
    for header in headers.iter() {
        // Determine column type from first non-empty value
        let mut column_data: Vec<String> = records.iter()
            .map(|r| r.get(header).unwrap_or(&"".to_string()).clone())
            .collect();
        
        // Try to parse as numbers
        let first_val = column_data.iter().find(|v| !v.is_empty());
        let (array, metadata) = if let Some(val) = first_val {
            // Try i64
            if let Ok(_) = val.parse::<i64>() {
                let ints: Vec<i64> = column_data.iter()
                    .filter_map(|v| v.parse::<i64>().ok())
                    .collect();
                let array = Arc::new(Int64Array::from(ints)) as Arc<dyn Array>;
                let metadata = FragmentMetadata {
                    row_count,
                    min_value: None,
                    max_value: None,
                    cardinality: row_count,
                    compression: CompressionType::None,
                    memory_size: row_count * 8,
                };
                (array, metadata)
            }
            // Try f64
            else if let Ok(_) = val.parse::<f64>() {
                let floats: Vec<f64> = column_data.iter()
                    .filter_map(|v| v.parse::<f64>().ok())
                    .collect();
                let array = Arc::new(Float64Array::from(floats)) as Arc<dyn Array>;
                let metadata = FragmentMetadata {
                    row_count,
                    min_value: None,
                    max_value: None,
                    cardinality: row_count,
                    compression: CompressionType::None,
                    memory_size: row_count * 8,
                };
                (array, metadata)
            }
            // String
            else {
                let array = Arc::new(StringArray::from(column_data)) as Arc<dyn Array>;
                let metadata = FragmentMetadata {
                    row_count,
                    min_value: None,
                    max_value: None,
                    cardinality: row_count,
                    compression: CompressionType::None,
                    memory_size: column_data.iter().map(|s| s.len()).sum::<usize>(),
                };
                (array, metadata)
            }
        } else {
            // Empty column - use strings
            let array = Arc::new(StringArray::from(column_data)) as Arc<dyn Array>;
            let metadata = FragmentMetadata {
                row_count,
                min_value: None,
                max_value: None,
                cardinality: 0,
                compression: CompressionType::None,
                memory_size: 0,
            };
            (array, metadata)
        };
        
        let fragment = ColumnFragment::new(array, metadata);
        columns.push((header.to_string(), fragment));
    }
    
    engine.load_table(table_name, columns)?;
    
    Ok(row_count)
}

pub async fn load_csv_to_postgres(
    csv_path: &str,
    table_name: &str,
    connection_string: &str,
) -> Result<usize> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await?;
    
    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });
    
    let mut reader = Reader::from_path(csv_path)?;
    let headers = reader.headers()?.clone();
    
    // Read all records
    let mut records: Vec<Vec<String>> = vec![];
    for result in reader.records() {
        let record = result?;
        records.push(record.iter().map(|s| s.to_string()).collect());
    }
    
    if records.is_empty() {
        return Ok(0);
    }
    
    let row_count = records.len();
    
    // Create table (drop if exists)
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    client.execute(&drop_sql, &[]).await?;
    
    // Create table with TEXT columns (simplified)
    let column_defs: Vec<String> = headers.iter()
        .map(|h| format!("\"{}\" TEXT", h.replace(" ", "_").replace("-", "_")))
        .collect();
    let create_sql = format!(
        "CREATE TABLE \"{}\" ({})",
        table_name,
        column_defs.join(", ")
    );
    client.execute(&create_sql, &[]).await?;
    
    // Insert data in batches
    let placeholders: Vec<String> = (0..headers.len())
        .map(|i| format!("${}", i + 1))
        .collect();
    let insert_sql = format!(
        "INSERT INTO \"{}\" VALUES ({})",
        table_name,
        placeholders.join(", ")
    );
    
    let stmt = client.prepare(&insert_sql).await?;
    for record in records {
        let params: Vec<&str> = record.iter().map(|s| s.as_str()).collect();
        client.execute(&stmt, &params).await?;
    }
    
    Ok(row_count)
}

