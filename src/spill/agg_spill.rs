/// AggSpill: Partitioned aggregation with spilling
/// 
/// This module provides spill functionality for aggregate operators,
/// implementing partitioned aggregation that can spill to disk.
use crate::error::EngineResult;
use crate::execution::vectorized::VectorBatch;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, debug, warn};
use fxhash::FxHashMap;
use crate::storage::fragment::Value;

/// Partitioned aggregation spill manager
pub struct AggSpill {
    /// Spill directory
    spill_dir: PathBuf,
    
    /// Partition count
    partition_count: usize,
    
    /// Aggregation partitions (spilled to disk)
    agg_partitions: Vec<Vec<PathBuf>>,
    
    /// Current partition being processed
    current_partition: usize,
    
    /// Memory limit per partition (bytes)
    memory_limit: usize,
    
    /// Current memory usage per partition
    partition_memory: Vec<usize>,
}

impl AggSpill {
    /// Create a new aggregation spill manager
    pub fn new(spill_dir: PathBuf, partition_count: usize, memory_limit: usize) -> Self {
        // Create spill directory if it doesn't exist
        std::fs::create_dir_all(&spill_dir).unwrap_or_else(|e| {
            warn!("Failed to create spill directory: {}", e);
        });
        
        Self {
            spill_dir,
            partition_count,
            agg_partitions: vec![Vec::new(); partition_count],
            current_partition: 0,
            memory_limit,
            partition_memory: vec![0; partition_count],
        }
    }
    
    /// Spill an aggregation batch to a partition
    pub fn spill_agg_batch(
        &mut self,
        batch: &VectorBatch,
        partition_idx: usize,
    ) -> EngineResult<PathBuf> {
        let file_path = self.get_partition_file(partition_idx);
        
        // Convert VectorBatch to RecordBatch
        let record_batch = batch.to_record_batch()?;
        
        // Write to Arrow IPC file
        self.write_batch_to_file(&file_path, &record_batch)?;
        
        // Track partition file
        self.agg_partitions[partition_idx].push(file_path.clone());
        
        // Update memory usage
        let batch_size = self.estimate_batch_size(batch);
        self.partition_memory[partition_idx] += batch_size;
        
        debug!(
            partition = partition_idx,
            file = ?file_path,
            rows = batch.row_count,
            memory_used = self.partition_memory[partition_idx],
            "Spilled aggregation batch to partition"
        );
        
        Ok(file_path)
    }
    
    /// Load an aggregation partition from disk
    pub fn load_agg_partition(
        &self,
        partition_idx: usize,
    ) -> EngineResult<Vec<VectorBatch>> {
        let mut batches = Vec::new();
        
        for file_path in &self.agg_partitions[partition_idx] {
            let batch = self.load_batch_from_file(file_path)?;
            batches.push(batch);
        }
        
        info!(
            partition = partition_idx,
            batch_count = batches.len(),
            "Loaded aggregation partition from disk"
        );
        
        Ok(batches)
    }
    
    /// Check if a partition needs spilling
    pub fn should_spill_partition(&self, partition_idx: usize, batch_size: usize) -> bool {
        self.partition_memory[partition_idx] + batch_size > self.memory_limit
    }
    
    /// Get next partition to process
    pub fn next_partition(&mut self) -> Option<usize> {
        if self.current_partition < self.partition_count {
            let partition = self.current_partition;
            self.current_partition += 1;
            Some(partition)
        } else {
            None
        }
    }
    
    /// Get partition file path
    fn get_partition_file(&self, partition_idx: usize) -> PathBuf {
        let file_name = format!("agg_partition_{}.arrow", partition_idx);
        self.spill_dir.join(file_name)
    }
    
    /// Write batch to Arrow IPC file
    fn write_batch_to_file(
        &self,
        file_path: &Path,
        batch: &RecordBatch,
    ) -> EngineResult<()> {
        let file = File::create(file_path)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create spill file: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        let mut writer = FileWriter::try_new(file, &batch.schema())
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create Arrow IPC writer: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        writer.write(batch)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to write batch to spill file: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        writer.finish()
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to finish writing spill file: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        Ok(())
    }
    
    /// Load batch from Arrow IPC file
    fn load_batch_from_file(&self, file_path: &Path) -> EngineResult<VectorBatch> {
        let file = File::open(file_path)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to open spill file: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        let mut reader = FileReader::try_new(file, None)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create Arrow IPC reader: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        // Read first batch (assuming one batch per file for simplicity)
        let batch = reader.next()
            .ok_or_else(|| crate::error::EngineError::IO {
                message: "Spill file is empty".to_string(),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: None,
            })?
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to read batch from spill file: {}", e),
                path: Some(file_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        // Convert RecordBatch to VectorBatch
        VectorBatch::from_record_batch(&batch)
            .map_err(|e| crate::error::EngineError::execution(format!(
                "Failed to convert RecordBatch to VectorBatch: {}", e
            )))
    }
    
    /// Estimate batch memory size
    fn estimate_batch_size(&self, batch: &VectorBatch) -> usize {
        batch.columns.iter()
            .map(|arr| {
                let len = arr.len();
                match arr.data_type() {
                    arrow::datatypes::DataType::Int64 => len * 8,
                    arrow::datatypes::DataType::Float64 => len * 8,
                    arrow::datatypes::DataType::Utf8 => len * 20, // Rough estimate
                    _ => len * 8,
                }
            })
            .sum()
    }
    
    /// Clean up spill files
    pub fn cleanup(&self) -> EngineResult<()> {
        for partition_files in &self.agg_partitions {
            for file_path in partition_files {
                if let Err(e) = std::fs::remove_file(file_path) {
                    warn!("Failed to remove spill file {:?}: {}", file_path, e);
                }
            }
        }
        info!("Cleaned up aggregation spill files");
        Ok(())
    }
}

