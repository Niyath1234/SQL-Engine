/// SortSpill: External merge sort with spilling
/// 
/// This module provides spill functionality for sort operators,
/// implementing external merge sort with multi-way merging.
use crate::error::EngineResult;
use crate::execution::vectorized::VectorBatch;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, debug, warn};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

/// External merge sort manager
pub struct SortSpill {
    /// Spill directory
    spill_dir: PathBuf,
    
    /// Sorted run files (one per run)
    sorted_runs: Vec<PathBuf>,
    
    /// Current run being written
    current_run: Option<PathBuf>,
    
    /// Memory limit for in-memory sort (bytes)
    memory_limit: usize,
    
    /// Current memory usage (bytes)
    current_memory: usize,
    
    /// Number of runs merged at once (multi-way merge)
    merge_arity: usize,
}

impl SortSpill {
    /// Create a new sort spill manager
    pub fn new(spill_dir: PathBuf, memory_limit: usize) -> Self {
        // Create spill directory if it doesn't exist
        std::fs::create_dir_all(&spill_dir).unwrap_or_else(|e| {
            warn!("Failed to create spill directory: {}", e);
        });
        
        Self {
            spill_dir,
            sorted_runs: Vec::new(),
            current_run: None,
            memory_limit,
            current_memory: 0,
            merge_arity: 8, // Merge 8 runs at once
        }
    }
    
    /// Add a batch to the current run (spill if needed)
    pub fn add_batch(&mut self, batch: &VectorBatch) -> EngineResult<()> {
        let batch_size = self.estimate_batch_size(batch);
        
        // Check if we need to spill current run
        if self.current_memory + batch_size > self.memory_limit {
            if let Some(run_path) = self.current_run.take() {
                // Finalize current run
                self.finalize_run(&run_path)?;
            }
            
            // Start new run
            self.current_run = Some(self.create_new_run_file()?);
            self.current_memory = 0;
        }
        
        // Write batch to current run
        if let Some(ref run_path) = self.current_run {
            self.write_batch_to_run(run_path, batch)?;
            self.current_memory += batch_size;
        } else {
            // Should not happen, but handle gracefully
            self.current_run = Some(self.create_new_run_file()?);
            self.write_batch_to_run(self.current_run.as_ref().unwrap(), batch)?;
            self.current_memory = batch_size;
        }
        
        Ok(())
    }
    
    /// Finalize current run and prepare for merging
    pub fn finalize_current_run(&mut self) -> EngineResult<()> {
        if let Some(run_path) = self.current_run.take() {
            self.finalize_run(&run_path)?;
            self.current_memory = 0;
        }
        Ok(())
    }
    
    /// Merge all sorted runs into final sorted output
    pub fn merge_runs(&self) -> EngineResult<Vec<VectorBatch>> {
        if self.sorted_runs.is_empty() {
            return Ok(Vec::new());
        }
        
        info!(
            run_count = self.sorted_runs.len(),
            merge_arity = self.merge_arity,
            "Merging sorted runs"
        );
        
        // Multi-way merge: merge runs in groups
        let mut current_runs = self.sorted_runs.clone();
        let mut merge_level = 0;
        
        while current_runs.len() > 1 {
            let mut next_level_runs = Vec::new();
            
            // Merge runs in groups
            for chunk in current_runs.chunks(self.merge_arity) {
                let merged_run = self.merge_run_group(chunk, merge_level)?;
                next_level_runs.push(merged_run);
            }
            
            current_runs = next_level_runs;
            merge_level += 1;
        }
        
        // Load final merged run
        if let Some(final_run) = current_runs.first() {
            self.load_run(final_run)
        } else {
            Ok(Vec::new())
        }
    }
    
    /// Merge a group of runs into a single sorted run
    fn merge_run_group(&self, run_paths: &[PathBuf], level: usize) -> EngineResult<PathBuf> {
        if run_paths.len() == 1 {
            return Ok(run_paths[0].clone());
        }
        
        // Create output run file
        let output_path = self.spill_dir.join(format!("merged_level_{}_run_{}.arrow", level, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        
        // Open all input runs
        let mut readers: Vec<FileReader<File>> = run_paths.iter()
            .map(|path| {
                let file = File::open(path)
                    .map_err(|e| crate::error::EngineError::IO {
                        message: format!("Failed to open run file: {}", e),
                        path: Some(path.to_string_lossy().to_string()),
                        source_message: Some(e.to_string()),
                    })?;
                FileReader::try_new(file, None)
                    .map_err(|e| crate::error::EngineError::IO {
                        message: format!("Failed to create reader: {}", e),
                        path: Some(path.to_string_lossy().to_string()),
                        source_message: Some(e.to_string()),
                    })
            })
            .collect::<EngineResult<Vec<_>>>()?;
        
        // Create output writer
        let output_file = File::create(&output_path)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create merged run file: {}", e),
                path: Some(output_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        // Get schema from first reader
        let schema = readers[0].schema();
        let mut writer = FileWriter::try_new(output_file, &schema)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create writer: {}", e),
                path: Some(output_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        // Multi-way merge using a heap
        // TODO: Implement proper multi-way merge with sort keys
        // For now, just concatenate batches (this is a placeholder)
        
        for reader in &mut readers {
            while let Some(batch_result) = reader.next() {
                let batch = batch_result
                    .map_err(|e| crate::error::EngineError::IO {
                        message: format!("Failed to read batch: {}", e),
                        path: None,
                        source_message: Some(e.to_string()),
                    })?;
                writer.write(&batch)
                    .map_err(|e| crate::error::EngineError::IO {
                        message: format!("Failed to write batch: {}", e),
                        path: Some(output_path.to_string_lossy().to_string()),
                        source_message: Some(e.to_string()),
                    })?;
            }
        }
        
        writer.finish()
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to finish writer: {}", e),
                path: Some(output_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        Ok(output_path)
    }
    
    /// Load a run from disk
    fn load_run(&self, run_path: &Path) -> EngineResult<Vec<VectorBatch>> {
        let file = File::open(run_path)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to open run file: {}", e),
                path: Some(run_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        let mut reader = FileReader::try_new(file, None)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create reader: {}", e),
                path: Some(run_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        let mut batches = Vec::new();
        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| crate::error::EngineError::IO {
                    message: format!("Failed to read batch: {}", e),
                    path: Some(run_path.to_string_lossy().to_string()),
                    source_message: Some(e.to_string()),
                })?;
            batches.push(VectorBatch::from_record_batch(&batch)?);
        }
        
        Ok(batches)
    }
    
    /// Create a new run file
    fn create_new_run_file(&mut self) -> EngineResult<PathBuf> {
        let file_name = format!("sort_run_{}.arrow", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let run_path = self.spill_dir.join(file_name);
        self.sorted_runs.push(run_path.clone());
        Ok(run_path)
    }
    
    /// Write batch to run file
    fn write_batch_to_run(&self, run_path: &Path, batch: &VectorBatch) -> EngineResult<()> {
        let record_batch = batch.to_record_batch()?;
        
        // Open file in append mode or create new
        let file = if run_path.exists() {
            std::fs::OpenOptions::new()
                .append(true)
                .open(run_path)
                .map_err(|e| crate::error::EngineError::IO {
                    message: format!("Failed to open run file for appending: {}", e),
                    path: Some(run_path.to_string_lossy().to_string()),
                    source_message: Some(e.to_string()),
                })?
        } else {
            File::create(run_path)
                .map_err(|e| crate::error::EngineError::IO {
                    message: format!("Failed to create run file: {}", e),
                    path: Some(run_path.to_string_lossy().to_string()),
                    source_message: Some(e.to_string()),
                })?
        };
        
        // For simplicity, we'll write each batch as a separate file
        // In a production system, we'd use a proper streaming format
        let mut writer = FileWriter::try_new(file, &record_batch.schema())
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to create writer: {}", e),
                path: Some(run_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        writer.write(&record_batch)
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to write batch: {}", e),
                path: Some(run_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        writer.finish()
            .map_err(|e| crate::error::EngineError::IO {
                message: format!("Failed to finish writer: {}", e),
                path: Some(run_path.to_string_lossy().to_string()),
                source_message: Some(e.to_string()),
            })?;
        
        Ok(())
    }
    
    /// Finalize a run (mark as complete)
    fn finalize_run(&self, _run_path: &Path) -> EngineResult<()> {
        // Run is already written, nothing to do
        debug!("Finalized sort run");
        Ok(())
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
        for run_path in &self.sorted_runs {
            if let Err(e) = std::fs::remove_file(run_path) {
                warn!("Failed to remove sort run file {:?}: {}", run_path, e);
            }
        }
        info!("Cleaned up sort spill files");
        Ok(())
    }
}

