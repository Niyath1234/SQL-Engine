/// Legacy configuration types for backward compatibility
/// These types are kept for compatibility with existing code
/// New code should use production::ProductionConfig

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Batch size for vectorized operations
    pub batch_size: usize,
    
    /// Memory limits
    pub memory: MemoryConfig,
    
    /// Spill configuration
    pub spill: SpillConfig,
    
    /// Join configuration
    pub join: JoinConfig,
    
    /// Scan configuration
    pub scan: ScanConfig,
    
    /// Execution configuration
    pub execution: ExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum memory per query in bytes
    pub max_memory_per_query: usize,
    
    /// Maximum memory for the entire engine
    pub max_engine_memory: usize,
    
    /// Memory pool size
    pub memory_pool_size: usize,
    
    /// Maximum memory per operator in bytes
    pub max_operator_memory_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpillConfig {
    /// Memory threshold in bytes before spilling to disk
    pub threshold_bytes: usize,
    
    /// Directory for spill files
    pub spill_dir: PathBuf,
    
    /// Number of partitions for join spill operations
    pub join_spill_partitions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinConfig {
    /// Hash join threshold
    pub hash_join_threshold: usize,
    
    /// Use radix partitioning for joins
    pub use_radix_partitioning: bool,
    
    /// Number of threads for parallel join build
    pub parallel_build_threads: usize,
    
    /// Enable parallel build for joins
    pub enable_parallel_build: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanConfig {
    /// Batch size for scans
    pub batch_size: usize,
    
    /// Use SIMD for scans
    pub use_simd: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Number of threads for parallel execution
    pub num_threads: usize,
    
    /// Enable vectorized execution
    pub enable_vectorized: bool,
    
    /// Maximum execution time in seconds
    pub max_execution_time_secs: u64,
    
    /// Maximum iterations for iterative algorithms
    pub max_iterations: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory: MemoryConfig::default(),
            spill: SpillConfig::default(),
            join: JoinConfig::default(),
            scan: ScanConfig::default(),
            execution: ExecutionConfig::default(),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_memory_per_query: 10 * 1024 * 1024 * 1024, // 10GB
            max_engine_memory: 100 * 1024 * 1024 * 1024, // 100GB
            memory_pool_size: 1000,
            max_operator_memory_bytes: 1 * 1024 * 1024 * 1024, // 1GB per operator
        }
    }
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 100 * 1024 * 1024, // 100MB
            spill_dir: std::env::temp_dir().join("hypergraph_spill"),
            join_spill_partitions: 16, // Default to 16 partitions for join spills
        }
    }
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            hash_join_threshold: 10000,
            use_radix_partitioning: true,
            parallel_build_threads: num_cpus::get(),
            enable_parallel_build: true,
        }
    }
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            use_simd: true,
        }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            num_threads: num_cpus::get(),
            enable_vectorized: true,
            max_execution_time_secs: 3600, // 1 hour default
            max_iterations: 1000, // Default max iterations for iterative algorithms
        }
    }
}

