/// LLM-optimized result formatting (High Priority #8)
/// Provides 90-99% token reduction for LLM queries
use crate::execution::batch::ExecutionBatch;
use crate::execution::engine::QueryResult;
use serde::{Deserialize, Serialize};

/// Result format options for LLM queries
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ResultFormat {
    /// Full results (all rows) - default
    Full,
    /// Summary only (metadata + statistics, no rows)
    Summary,
    /// Sample N rows + summary
    Sample(usize),
    /// Representative rows (diverse values)
    Representative(usize),
    /// Metadata only (schema + row count)
    Metadata,
}

impl Default for ResultFormat {
    fn default() -> Self {
        ResultFormat::Full
    }
}

/// Formatted result for LLM consumption
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FormattedResult {
    /// Summary text (for Summary/Sample/Metadata modes)
    pub summary: Option<String>,
    /// Sample rows (for Sample/Representative modes)
    pub sample_rows: Vec<Vec<String>>,
    /// Full rows (for Full mode only)
    pub full_rows: Vec<Vec<String>>,
    /// Column names
    pub columns: Vec<String>,
    /// Total row count
    pub row_count: usize,
    /// Whether this is a summary (not full results)
    pub is_summary: bool,
}

/// Format query results based on ResultFormat
pub fn format_results(
    result: &QueryResult,
    format: ResultFormat,
    column_names: Vec<String>,
) -> FormattedResult {
    // Convert batches to rows
    let all_rows = batches_to_rows(result, &column_names);
    
    match format {
        ResultFormat::Full => {
            FormattedResult {
                summary: None,
                sample_rows: vec![],
                full_rows: all_rows,
                columns: column_names,
                row_count: result.row_count,
                is_summary: false,
            }
        }
        ResultFormat::Summary => {
            let summary = generate_summary(result, &column_names, &all_rows);
            FormattedResult {
                summary: Some(summary),
                sample_rows: vec![],
                full_rows: vec![],
                columns: column_names,
                row_count: result.row_count,
                is_summary: true,
            }
        }
        ResultFormat::Sample(n) => {
            let summary = generate_summary(result, &column_names, &all_rows);
            let sample = all_rows.into_iter().take(n).collect();
            FormattedResult {
                summary: Some(summary),
                sample_rows: sample,
                full_rows: vec![],
                columns: column_names,
                row_count: result.row_count,
                is_summary: true,
            }
        }
        ResultFormat::Representative(n) => {
            let summary = generate_summary(result, &column_names, &all_rows);
            let representative = select_representative_rows(&all_rows, n);
            FormattedResult {
                summary: Some(summary),
                sample_rows: representative,
                full_rows: vec![],
                columns: column_names,
                row_count: result.row_count,
                is_summary: true,
            }
        }
        ResultFormat::Metadata => {
            let summary = format!("Table has {} rows, {} columns: [{}]", 
                result.row_count, 
                column_names.len(),
                column_names.join(", "));
            FormattedResult {
                summary: Some(summary),
                sample_rows: vec![],
                full_rows: vec![],
                columns: column_names,
                row_count: result.row_count,
                is_summary: true,
            }
        }
    }
}

/// Convert execution batches to rows
fn batches_to_rows(result: &QueryResult, column_names: &[String]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    
    for batch in &result.batches {
        let row_count = batch.row_count;
        let col_count = column_names.len();
        
        for row_idx in 0..row_count {
            if !batch.selection[row_idx] {
                continue;
            }
            
            let mut row = Vec::new();
            for col_idx in 0..col_count {
                if let Some(array) = batch.batch.column(col_idx) {
                    let value = format_value(array, row_idx);
                    row.push(value);
                } else {
                    row.push("NULL".to_string());
                }
            }
            rows.push(row);
        }
    }
    
    rows
}

/// Format a single value from an array
fn format_value(array: &arrow::array::ArrayRef, idx: usize) -> String {
    use arrow::array::*;
    
    match array.data_type() {
        arrow::datatypes::DataType::Int64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                if arr.is_null(idx) {
                    "NULL".to_string()
                } else {
                    arr.value(idx).to_string()
                }
            } else {
                "NULL".to_string()
            }
        }
        arrow::datatypes::DataType::Float64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                if arr.is_null(idx) {
                    "NULL".to_string()
                } else {
                    arr.value(idx).to_string()
                }
            } else {
                "NULL".to_string()
            }
        }
        arrow::datatypes::DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if arr.is_null(idx) {
                    "NULL".to_string()
                } else {
                    arr.value(idx).to_string()
                }
            } else {
                "NULL".to_string()
            }
        }
        _ => format!("{:?}", array),
    }
}

/// Generate summary text for results
fn generate_summary(
    result: &QueryResult,
    column_names: &[String],
    rows: &[Vec<String>],
) -> String {
    if rows.is_empty() {
        return format!("Query returned 0 rows. Columns: [{}]", column_names.join(", "));
    }
    
    // Calculate statistics for numeric columns
    let mut stats = Vec::new();
    for (col_idx, col_name) in column_names.iter().enumerate() {
        if let Some(first_row) = rows.first() {
            if col_idx < first_row.len() {
                // Try to parse as number
                if let Ok(num) = first_row[col_idx].parse::<f64>() {
                    let values: Vec<f64> = rows.iter()
                        .filter_map(|r| r.get(col_idx).and_then(|v| v.parse::<f64>().ok()))
                        .collect();
                    if !values.is_empty() {
                        let sum: f64 = values.iter().sum();
                        let avg = sum / values.len() as f64;
                        let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                        let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                        stats.push(format!("{}: avg={:.2}, min={:.2}, max={:.2}", col_name, avg, min, max));
                    }
                }
            }
        }
    }
    
    let stats_text = if stats.is_empty() {
        String::new()
    } else {
        format!(" Statistics: {}", stats.join("; "))
    };
    
    format!("Query returned {} rows, {} columns: [{}].{}", 
        result.row_count,
        column_names.len(),
        column_names.join(", "),
        stats_text)
}

/// Select representative rows (diverse values)
fn select_representative_rows(rows: &[Vec<String>], n: usize) -> Vec<Vec<String>> {
    if rows.len() <= n {
        return rows.to_vec();
    }
    
    // Simple strategy: take first, middle, and last rows
    let mut selected = Vec::new();
    
    // First row
    if !rows.is_empty() {
        selected.push(rows[0].clone());
    }
    
    // Middle rows
    let step = rows.len() / (n - 1).max(1);
    for i in (step..rows.len()).step_by(step) {
        if selected.len() >= n {
            break;
        }
        selected.push(rows[i].clone());
    }
    
    // Last row
    if selected.len() < n && rows.len() > 1 {
        selected.push(rows[rows.len() - 1].clone());
    }
    
    selected
}

