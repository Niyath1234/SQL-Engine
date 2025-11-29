/// Parallel merge for spill runs
/// Merges multiple compressed spill runs in parallel to reduce IO blocking
use tokio::task;
use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::FileReader;
use anyhow::Result;
use std::io::Cursor;

/// Decompress LZ4 data
/// Uses lz4 crate API for decompression
fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    use lz4::Decoder;
    use std::io::Read;
    
    let mut decoder = Decoder::new(data)
        .map_err(|e| anyhow::anyhow!("Failed to create LZ4 decoder: {}", e))?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)
        .map_err(|e| anyhow::anyhow!("Failed to decompress: {}", e))?;
    Ok(decompressed)
}

/// Merge multiple spill runs in parallel
/// Decompresses and deserializes runs concurrently, then merges the batches
pub async fn parallel_merge(runs: Vec<Vec<u8>>) -> Result<RecordBatch> {
    if runs.is_empty() {
        anyhow::bail!("Cannot merge empty runs");
    }
    
    // Decompress and deserialize runs in parallel
    let mut handles = vec![];
    for run in runs {
        handles.push(task::spawn_blocking(move || -> Result<RecordBatch> {
            // Decompress LZ4 data
            let decompressed = decompress_lz4(&run)
                .map_err(|e| anyhow::anyhow!("Failed to decompress spill run: {}", e))?;
            
            // Deserialize Arrow IPC
            let cursor = Cursor::new(decompressed);
            let reader = FileReader::try_new(cursor, None)
                .map_err(|e| anyhow::anyhow!("Failed to create Arrow reader: {}", e))?;
            
            // Read all batches from the run
            let mut batches = Vec::new();
            for batch_result in reader {
                batches.push(batch_result?);
            }
            
            // If multiple batches in run, concatenate them
            if batches.is_empty() {
                anyhow::bail!("No batches found in spill run");
            }
            
            if batches.len() == 1 {
                Ok(batches.into_iter().next().unwrap())
            } else {
                // Concatenate multiple batches
                use arrow::compute::concat_batches;
                let schema = batches[0].schema();
                concat_batches(&schema, &batches)
                    .map_err(|e| anyhow::anyhow!("Failed to concatenate batches: {}", e))
            }
        }));
    }
    
    // Collect all batches
    let mut batches = Vec::new();
    for h in handles {
        batches.push(h.await??);
    }
    
    // Merge all batches into a single batch
    if batches.is_empty() {
        anyhow::bail!("No batches to merge");
    }
    
    if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        // Concatenate all batches
        use arrow::compute::concat_batches;
        let schema = batches[0].schema();
        concat_batches(&schema, &batches)
            .map_err(|e| anyhow::anyhow!("Failed to merge spill runs: {}", e))
    }
}

/// Merge spill runs with a limit (for partial results)
pub async fn parallel_merge_with_limit(runs: Vec<Vec<u8>>, limit: usize) -> Result<RecordBatch> {
    let merged = parallel_merge(runs).await?;
    
    // Apply limit
    if merged.num_rows() > limit {
        Ok(merged.slice(0, limit))
    } else {
        Ok(merged)
    }
}

