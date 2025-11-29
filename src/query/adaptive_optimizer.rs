/// Runtime adaptive optimization hooks
/// Allows executor to provide feedback and trigger reoptimization
use crate::query::plan::PlanOperator;
use crate::query::memo::Cost;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

/// Runtime statistics collected during execution
#[derive(Clone, Debug)]
pub struct RuntimeStatistics {
    /// Actual cardinality observed
    pub actual_cardinality: usize,
    
    /// Actual execution time in milliseconds
    pub execution_time_ms: f64,
    
    /// Actual memory used in bytes
    pub memory_used_bytes: usize,
    
    /// Number of rows spilled to disk
    pub rows_spilled: usize,
    
    /// Operator-specific statistics
    pub operator_stats: HashMap<String, OperatorStats>,
}

/// Statistics for a specific operator
#[derive(Clone, Debug)]
pub struct OperatorStats {
    /// Input cardinality
    pub input_cardinality: usize,
    
    /// Output cardinality
    pub output_cardinality: usize,
    
    /// Execution time
    pub execution_time_ms: f64,
    
    /// Memory used
    pub memory_bytes: usize,
}

/// Adaptive optimizer that can reoptimize based on runtime feedback
pub struct AdaptiveOptimizer {
    /// Collected runtime statistics
    pub runtime_stats: Arc<Mutex<Vec<RuntimeStatistics>>>,
    
    /// Threshold for triggering reoptimization (error percentage)
    pub reoptimize_threshold: f64,
    
    /// Whether adaptive optimization is enabled
    pub enabled: bool,
}

impl AdaptiveOptimizer {
    pub fn new() -> Self {
        Self {
            runtime_stats: Arc::new(Mutex::new(Vec::new())),
            reoptimize_threshold: 0.5, // 50% error threshold
            enabled: true,
        }
    }
    
    /// Record runtime statistics
    pub fn record_runtime_stats(&self, stats: RuntimeStatistics) {
        if self.enabled {
            if let Ok(mut runtime_stats) = self.runtime_stats.lock() {
                runtime_stats.push(stats);
                
                // Keep only recent statistics (last 1000 queries)
                if runtime_stats.len() > 1000 {
                    runtime_stats.remove(0);
                }
            }
        }
    }
    
    /// Check if reoptimization is needed based on runtime feedback
    pub fn should_reoptimize(
        &self,
        estimated_cardinality: f64,
        actual_cardinality: usize,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        
        if estimated_cardinality == 0.0 {
            return actual_cardinality > 0;
        }
        
        let error_ratio = (estimated_cardinality - actual_cardinality as f64).abs() / estimated_cardinality;
        error_ratio > self.reoptimize_threshold
    }
    
    /// Get runtime statistics for learning
    pub fn get_runtime_stats(&self) -> Vec<RuntimeStatistics> {
        if let Ok(runtime_stats) = self.runtime_stats.lock() {
            runtime_stats.clone()
        } else {
            Vec::new()
        }
    }
    
    /// Suggest join strategy change based on runtime feedback
    pub fn suggest_join_strategy_change(
        &self,
        current_strategy: &str,
        left_cardinality: usize,
        right_cardinality: usize,
        memory_pressure: f64,
    ) -> Option<String> {
        if !self.enabled {
            return None;
        }
        
        // If memory pressure is high, suggest hash join -> sort-merge join
        if memory_pressure > 0.8 && current_strategy == "hash" {
            return Some("sort_merge".to_string());
        }
        
        // If one side is very small, suggest nested loop
        if left_cardinality < 100 || right_cardinality < 100 {
            if current_strategy != "nested_loop" {
                return Some("nested_loop".to_string());
            }
        }
        
        None
    }
    
    /// Suggest hash table partitioning change
    pub fn suggest_partitioning_change(
        &self,
        current_partitions: usize,
        memory_pressure: f64,
        spill_count: usize,
    ) -> Option<usize> {
        if !self.enabled {
            return None;
        }
        
        // If spilling too much, increase partitions
        if spill_count > 1000 && memory_pressure > 0.7 {
            return Some(current_partitions * 2);
        }
        
        // If memory pressure is low, can reduce partitions
        if memory_pressure < 0.3 && current_partitions > 1 {
            return Some(current_partitions / 2);
        }
        
        None
    }
    
    /// Enable/disable adaptive optimization
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
    
    /// Set reoptimization threshold
    pub fn set_threshold(&mut self, threshold: f64) {
        self.reoptimize_threshold = threshold.max(0.0).min(1.0);
    }
}

impl Default for AdaptiveOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Hook for executor to provide runtime feedback
pub trait AdaptiveExecutionHook {
    /// Called when operator starts execution
    fn on_operator_start(&self, operator: &PlanOperator);
    
    /// Called when operator completes execution
    fn on_operator_complete(
        &self,
        operator: &PlanOperator,
        stats: OperatorStats,
    );
    
    /// Called when memory pressure is detected
    fn on_memory_pressure(&self, pressure: f64);
    
    /// Called when spill occurs
    fn on_spill(&self, rows_spilled: usize);
}

/// Default implementation of adaptive execution hook
pub struct DefaultAdaptiveHook {
    optimizer: Arc<AdaptiveOptimizer>,
}

impl DefaultAdaptiveHook {
    pub fn new(optimizer: Arc<AdaptiveOptimizer>) -> Self {
        Self { optimizer }
    }
}

impl AdaptiveExecutionHook for DefaultAdaptiveHook {
    fn on_operator_start(&self, _operator: &PlanOperator) {
        // Could track start time, etc.
    }
    
    fn on_operator_complete(
        &self,
        _operator: &PlanOperator,
        stats: OperatorStats,
    ) {
        // Record statistics
        let runtime_stats = RuntimeStatistics {
            actual_cardinality: stats.output_cardinality,
            execution_time_ms: stats.execution_time_ms,
            memory_used_bytes: stats.memory_bytes,
            rows_spilled: 0, // Would be tracked separately
            operator_stats: HashMap::new(),
        };
        
        self.optimizer.record_runtime_stats(runtime_stats);
    }
    
    fn on_memory_pressure(&self, _pressure: f64) {
        // Could trigger early spill, etc.
    }
    
    fn on_spill(&self, _rows_spilled: usize) {
        // Track spill events
    }
}

