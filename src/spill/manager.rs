/// Spill-to-disk manager for handling large queries that exceed memory limits
/// Monitors memory usage and spills batches to disk when threshold is exceeded
use tokio::fs;
use tokio::io::AsyncWriteExt;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use serde::{Serialize, Deserialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::Result;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpillConfig {
    /// Memory threshold in bytes before spilling to disk
    pub threshold_bytes: usize,
    /// Directory for spill files
    pub spill_dir: PathBuf,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 100 * 1024 * 1024, // 100 MB default
            spill_dir: std::env::temp_dir().join("hypergraph_spill"),
        }
    }
}

/// Spill manager that monitors memory usage and spills batches to disk
pub struct SpillManager {
    cfg: SpillConfig,
    current_bytes: usize,
    spill_files: Vec<PathBuf>,
    /// Pending spill runs (for advanced spill with compression)
    pending_runs: Vec<crate::spill::run_builder::SpillRunBuilder>,
    /// Predictive controller for proactive spill management
    pub predictive: crate::spill::predictive_controller::PredictiveController,
}

impl SpillManager {
    /// Create a new spill manager with default configuration
    pub fn new(threshold_bytes: usize) -> Self {
        Self::with_config(SpillConfig {
            threshold_bytes,
            ..Default::default()
        })
    }
    
    /// Create a new spill manager with custom configuration
    pub fn with_config(cfg: SpillConfig) -> Self {
        // Ensure spill directory exists
        if let Err(e) = std::fs::create_dir_all(&cfg.spill_dir) {
            eprintln!("Warning: Failed to create spill directory {:?}: {}", cfg.spill_dir, e);
        }
        
        Self {
            cfg,
            current_bytes: 0,
            spill_files: Vec::new(),
            pending_runs: Vec::new(),
            predictive: crate::spill::predictive_controller::PredictiveController::new(),
        }
    }
    
    /// Check if a batch should be spilled and spill it if necessary
    /// Returns the spill file path if batch was spilled, None otherwise
    pub async fn maybe_spill(&mut self, batch: &RecordBatch) -> Result<Option<PathBuf>> {
        // Estimate memory size of the batch
        let batch_size = self.estimate_batch_size(batch);
        self.current_bytes += batch_size;
        
        // Check if we've exceeded the threshold
        if self.current_bytes < self.cfg.threshold_bytes {
            return Ok(None);
        }
        
        // ADVANCED SPILL: Use run builder for compressed spill runs
        let mut run_builder = crate::spill::run_builder::SpillRunBuilder::new();
        run_builder.add_batch(batch)?;
        self.pending_runs.push(run_builder);
        
        // Spill the batch to disk (legacy path for now)
        let spill_path = self.spill_batch(batch).await?;
        self.spill_files.push(spill_path.clone());
        
        // Reset current bytes after spill
        self.current_bytes = 0;
        
        Ok(Some(spill_path))
    }
    
    /// Spill a batch to disk using Arrow IPC format
    async fn spill_batch(&self, batch: &RecordBatch) -> Result<PathBuf> {
        // Generate unique spill file name
        let file_name = format!("spill_{}.arrow", uuid::Uuid::new_v4());
        let spill_path = self.cfg.spill_dir.join(file_name);
        
        // Write batch to disk asynchronously
        let schema = batch.schema();
        let _file = fs::File::create(&spill_path).await?;
        
        // Use Arrow IPC format for efficient serialization
        // Note: Arrow IPC writer is synchronous, so we'll use blocking task
        let schema_clone = schema.clone();
        let batch_clone = batch.clone();
        let path_clone = spill_path.clone();
        
        tokio::task::spawn_blocking(move || -> Result<()> {
            use arrow::ipc::writer::FileWriter;
            use std::fs::File;
            
            let file = File::create(&path_clone)?;
            let mut writer = FileWriter::try_new(file, &*schema_clone)?;
            writer.write(&batch_clone)?;
            writer.finish()?;
            Ok(())
        })
        .await??;
        
        Ok(spill_path)
    }
    
    /// Estimate the memory size of a batch
    fn estimate_batch_size(&self, batch: &RecordBatch) -> usize {
        // Estimate based on column count and row count
        // This is a rough estimate - actual size may vary
        let mut total_size = 0;
        for column in batch.columns() {
            // Rough estimate: 8 bytes per value for numeric types, more for strings
            total_size += column.len() * 8;
        }
        total_size
    }
    
    /// Get the current memory usage estimate
    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }
    
    /// Get the list of spill files created
    pub fn spill_files(&self) -> &[PathBuf] {
        &self.spill_files
    }
    
    /// Clean up spill files
    pub async fn cleanup(&self) -> Result<()> {
        for spill_file in &self.spill_files {
            if spill_file.exists() {
                if let Err(e) = fs::remove_file(spill_file).await {
                    eprintln!("Warning: Failed to remove spill file {:?}: {}", spill_file, e);
                }
            }
        }
        Ok(())
    }
    
    /// Read a batch from a spill file
    pub async fn read_spill(&self, spill_path: &Path) -> Result<RecordBatch> {
        let path = spill_path.to_path_buf();
        
        tokio::task::spawn_blocking(move || -> Result<RecordBatch> {
            use arrow::ipc::reader::FileReader;
            use std::fs::File;
            
            let file = File::open(&path)?;
            let mut reader = FileReader::try_new(file, None)?;
            
            // Read first batch (for now, assume single batch per file)
            if let Some(batch_result) = reader.next() {
                Ok(batch_result?)
            } else {
                anyhow::bail!("No batch found in spill file: {:?}", path)
            }
        })
        .await?
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        // Clean up spill files on drop
        // Note: This is synchronous, but it's in drop so we can't use async
        for spill_file in &self.spill_files {
            if spill_file.exists() {
                if let Err(e) = std::fs::remove_file(spill_file) {
                    eprintln!("Warning: Failed to remove spill file {:?}: {}", spill_file, e);
                }
            }
        }
    }
}

