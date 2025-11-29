/// Spill run builder for creating compressed spill runs
/// Groups multiple batches into compressed runs for efficient storage
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use anyhow::Result;

/// Compress data with LZ4
/// Uses lz4 crate API for compression
fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    use lz4::EncoderBuilder;
    use std::io::Write;
    
    let buffer = Vec::new();
    let mut encoder = EncoderBuilder::new()
        .level(4) // Compression level (0-16, 4 is default)
        .build(buffer)?;
    
    encoder.write_all(data)?;
    let (compressed, result) = encoder.finish();
    result?;
    Ok(compressed)
}

/// Builder for spill runs (groups of compressed batches)
pub struct SpillRunBuilder {
    /// Compressed batches in this run
    pub runs: Vec<Vec<u8>>,
    /// Schema for all batches in this run (must be consistent)
    schema: Option<arrow::datatypes::SchemaRef>,
}

impl SpillRunBuilder {
    /// Create a new spill run builder
    pub fn new() -> Self {
        Self {
            runs: Vec::new(),
            schema: None,
        }
    }
    
    /// Add a batch to the spill run
    /// Batches are serialized to Arrow IPC format and compressed with LZ4
    pub fn add_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Store schema from first batch
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        
        // Serialize batch to Arrow IPC format
        let mut buf = Vec::new();
        let mut writer = FileWriter::try_new(&mut buf, &*batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
        
        // Compress with LZ4 for efficient storage
        let compressed = compress_lz4(&buf)?;
        self.runs.push(compressed);
        
        Ok(())
    }
    
    /// Get the number of batches in this run
    pub fn len(&self) -> usize {
        self.runs.len()
    }
    
    /// Check if the run is empty
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
    
    /// Get the schema for this run
    pub fn schema(&self) -> Option<&arrow::datatypes::SchemaRef> {
        self.schema.as_ref()
    }
    
    /// Finish building and return the compressed runs
    pub fn finish(self) -> Vec<Vec<u8>> {
        self.runs
    }
}

impl Default for SpillRunBuilder {
    fn default() -> Self {
        Self::new()
    }
}

