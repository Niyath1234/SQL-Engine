/// Result serialization and output formatting
use crate::execution::batch::ExecutionBatch;
use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::array::Array;
use bitvec::prelude::*;
use std::sync::Arc;
use std::path::PathBuf;
use anyhow::Result;

/// Convert ExecutionBatch to Arrow RecordBatch
pub fn execution_batch_to_record_batch(batch: &ExecutionBatch) -> Result<RecordBatch> {
    // Apply selection vector to columns
    let mut columns = vec![];
    
    for col_idx in 0..batch.batch.columns.len() {
        let array = batch.batch.column(col_idx).ok_or_else(|| anyhow::anyhow!("Column {} not found", col_idx))?;
        let selected = apply_selection(array, &batch.selection)?;
        columns.push(selected);
    }
    
    RecordBatch::try_new(batch.batch.schema.clone(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}

/// Apply selection vector to array
fn apply_selection(array: &std::sync::Arc<dyn Array>, selection: &BitVec) -> Result<std::sync::Arc<dyn Array>> {
    // Filter array based on selection
    let builder: std::sync::Arc<dyn Array> = match array.data_type() {
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for (i, selected) in selection.iter().enumerate() {
                if *selected && !array.is_null(i) {
                    b.append_value(array.as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to cast"))?
                        .value(i));
                } else if *selected {
                    b.append_null();
                }
            }
            std::sync::Arc::new(b.finish()) as std::sync::Arc<dyn Array>
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for (i, selected) in selection.iter().enumerate() {
                if *selected && !array.is_null(i) {
                    b.append_value(array.as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to cast"))?
                        .value(i));
                } else if *selected {
                    b.append_null();
                }
            }
            std::sync::Arc::new(b.finish()) as std::sync::Arc<dyn Array>
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for (i, selected) in selection.iter().enumerate() {
                if *selected && !array.is_null(i) {
                    b.append_value(array.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| anyhow::anyhow!("Failed to cast"))?
                        .value(i));
                } else if *selected {
                    b.append_null();
                }
            }
            std::sync::Arc::new(b.finish()) as std::sync::Arc<dyn Array>
        }
        _ => {
            // Generic fallback - just return original for now
            array.clone()
        }
    };
    
    Ok(builder)
}

/// Convert multiple ExecutionBatches to RecordBatches
pub fn batches_to_record_batches(batches: &[ExecutionBatch]) -> Result<Vec<RecordBatch>> {
    let mut result = vec![];
    for batch in batches {
        result.push(execution_batch_to_record_batch(batch)?);
    }
    Ok(result)
}

/// Export results to Parquet
pub fn export_to_parquet(
    batches: &[RecordBatch],
    path: &str,
) -> Result<()> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props))?;
    
    for batch in batches {
        writer.write(batch)?;
    }
    
    writer.close()?;
    Ok(())
}

