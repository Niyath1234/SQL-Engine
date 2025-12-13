/// Phase 1: Vectorized, Async Operator Model
/// 
/// This module introduces the new operator architecture:
/// - VectorOperator: Async, vectorized operator trait
/// - VectorBatch: Arrow-backed batch with selection bitmap
/// - PipelinedOperator: Operators that maintain internal state
/// 
/// This replaces the synchronous BatchIterator model with an async, 
/// vectorized, pipelined model for better performance and scalability.

pub mod operator;
pub mod batch;
pub mod pipeline;
pub mod scan;
pub mod filter;
pub mod project;
pub mod join;
pub mod aggregate;
pub mod sort;
pub mod limit;
pub mod distinct;
pub mod engine;
pub mod builder;
pub mod integration;
pub mod radix_partition;
pub mod parallel_join;
pub mod work_stealing;
pub mod pipeline_builder;
pub mod driver_pool;
pub mod kernels;
pub mod kernels_simd;
pub mod kernel_fusion;
pub mod vectorized_filter;
pub mod vectorized_project;
pub mod parallel_scan;
pub mod enhanced_sort;
pub mod streaming_window;
pub mod enhanced_aggregate;

// Spill integration modules (foundation for future integration)
pub mod join_spill_integration;
pub mod sort_spill_integration;
pub mod agg_spill_integration;

#[cfg(test)]
pub mod test_utils;

// Legacy modules (for backward compatibility during migration)
pub mod scan_filter;

pub use operator::VectorOperator;
pub use batch::VectorBatch;
pub use pipeline::PipelinedOperator;
pub use scan::VectorScanOperator;
pub use filter::VectorFilterOperator;
pub use project::VectorProjectOperator;
pub use join::VectorJoinOperator;
pub use aggregate::VectorAggregateOperator;
pub use sort::VectorSortOperator;
pub use limit::VectorLimitOperator;
pub use distinct::VectorDistinctOperator;
pub use engine::VectorExecutionEngine;
pub use builder::{build_vector_operator_tree, vector_batches_to_execution_batches};
pub use integration::{execute_vectorized, is_vectorized_supported};
pub use pipeline_builder::{PipelineBuilder, ExecutionPipeline, PipelineState};
pub use driver_pool::{DriverThreadPool, Driver};
