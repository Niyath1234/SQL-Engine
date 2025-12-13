/// Unified MemoryPool: Per-query memory budgeting and tracking
/// 
/// This module provides a unified memory management system for tracking
/// memory usage across operators and automatically triggering spills.
use crate::error::EngineResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, debug, warn};
use std::collections::HashMap;

/// Memory pool for tracking and managing memory usage
pub struct MemoryPool {
    /// Total memory limit (bytes)
    total_limit: usize,
    
    /// Current memory usage (bytes)
    current_usage: AtomicUsize,
    
    /// Memory usage per operator (operator_id -> bytes)
    operator_usage: Arc<tokio::sync::RwLock<HashMap<String, usize>>>,
    
    /// Spill threshold (as fraction of total_limit)
    spill_threshold: f64,
    
    /// Whether memory pool is enabled
    enabled: bool,
}

impl MemoryPool {
    /// Create a new memory pool
    pub fn new(total_limit: usize, spill_threshold: f64) -> Self {
        Self {
            total_limit,
            current_usage: AtomicUsize::new(0),
            operator_usage: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            spill_threshold,
            enabled: true,
        }
    }
    
    /// Allocate memory for an operator
    pub async fn allocate(&self, operator_id: &str, bytes: usize) -> EngineResult<bool> {
        if !self.enabled {
            return Ok(true); // Memory pool disabled, allow allocation
        }
        
        let current = self.current_usage.load(Ordering::Relaxed);
        let new_total = current + bytes;
        
        // Check if allocation would exceed limit
        let threshold_bytes = (self.total_limit as f64 * self.spill_threshold) as usize;
        if new_total > threshold_bytes {
            warn!(
                operator = operator_id,
                requested = bytes,
                current = current,
                limit = self.total_limit,
                threshold = threshold_bytes,
                "Memory allocation would exceed threshold, spill may be triggered"
            );
            return Ok(false); // Indicate spill should be triggered
        }
        
        // Update usage
        self.current_usage.store(new_total, Ordering::Relaxed);
        
        // Update per-operator usage
        let mut usage = self.operator_usage.write().await;
        *usage.entry(operator_id.to_string()).or_insert(0) += bytes;
        
        debug!(
            operator = operator_id,
            allocated = bytes,
            total_usage = new_total,
            "Memory allocated"
        );
        
        Ok(true)
    }
    
    /// Deallocate memory for an operator
    pub async fn deallocate(&self, operator_id: &str, bytes: usize) -> EngineResult<()> {
        if !self.enabled {
            return Ok(());
        }
        
        let current = self.current_usage.load(Ordering::Relaxed);
        let new_total = current.saturating_sub(bytes);
        self.current_usage.store(new_total, Ordering::Relaxed);
        
        // Update per-operator usage
        let mut usage = self.operator_usage.write().await;
        if let Some(op_usage) = usage.get_mut(operator_id) {
            *op_usage = op_usage.saturating_sub(bytes);
            if *op_usage == 0 {
                usage.remove(operator_id);
            }
        }
        
        debug!(
            operator = operator_id,
            deallocated = bytes,
            total_usage = new_total,
            "Memory deallocated"
        );
        
        Ok(())
    }
    
    /// Check if memory pressure requires spilling
    pub fn should_spill(&self) -> bool {
        if !self.enabled {
            return false;
        }
        
        let current = self.current_usage.load(Ordering::Relaxed);
        let threshold_bytes = (self.total_limit as f64 * self.spill_threshold) as usize;
        current > threshold_bytes
    }
    
    /// Get current memory usage
    pub fn current_usage(&self) -> usize {
        self.current_usage.load(Ordering::Relaxed)
    }
    
    /// Get memory usage for a specific operator
    pub async fn operator_usage(&self, operator_id: &str) -> usize {
        let usage = self.operator_usage.read().await;
        usage.get(operator_id).copied().unwrap_or(0)
    }
    
    /// Get memory usage breakdown
    pub async fn usage_breakdown(&self) -> HashMap<String, usize> {
        let usage = self.operator_usage.read().await;
        usage.clone()
    }
    
    /// Reset memory pool (for new query)
    pub fn reset(&self) {
        self.current_usage.store(0, Ordering::Relaxed);
        // Note: operator_usage is cleared asynchronously
    }
    
    /// Enable or disable memory pool
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
    
    /// Get total memory limit
    pub fn get_total_limit(&self) -> usize {
        self.total_limit
    }
    
    /// Get current memory usage (async version for consistency)
    pub async fn get_current_usage(&self) -> EngineResult<usize> {
        Ok(self.current_usage.load(Ordering::Relaxed))
    }
    
    /// Get available memory
    pub async fn get_available_memory(&self) -> EngineResult<usize> {
        let used = self.current_usage.load(Ordering::Relaxed);
        let threshold_bytes = (self.total_limit as f64 * self.spill_threshold) as usize;
        Ok(threshold_bytes.saturating_sub(used))
    }
}

/// Batch arena for recycling batches
pub struct BatchArena {
    /// Pool of available batches (by size)
    available_batches: Arc<tokio::sync::RwLock<HashMap<usize, Vec<Vec<u8>>>>>,
    
    /// Maximum batches to keep per size
    max_batches_per_size: usize,
}

impl BatchArena {
    /// Create a new batch arena
    pub fn new(max_batches_per_size: usize) -> Self {
        Self {
            available_batches: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_batches_per_size,
        }
    }
    
    /// Get a batch from the arena (or create new)
    pub async fn get_batch(&self, size: usize) -> Vec<u8> {
        let mut batches = self.available_batches.write().await;
        if let Some(pool) = batches.get_mut(&size) {
            if let Some(batch) = pool.pop() {
                return batch;
            }
        }
        
        // No available batch, create new
        Vec::with_capacity(size)
    }
    
    /// Return a batch to the arena for recycling
    pub async fn return_batch(&self, mut batch: Vec<u8>, size: usize) {
        // Clear batch but keep capacity
        batch.clear();
        
        let mut batches = self.available_batches.write().await;
        let pool = batches.entry(size).or_insert_with(Vec::new);
        
        // Only keep limited number of batches
        if pool.len() < self.max_batches_per_size {
            pool.push(batch);
        }
    }
}

