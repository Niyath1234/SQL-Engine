pub mod engine;
pub mod hardware_tuning;
pub mod operators;
pub mod batch;
pub mod memory_pool;
pub mod wcoj;
pub mod simd_kernels;
pub mod simd_avx2;
pub mod fused_scan;
pub mod radix_join;
// DEPRECATED: bitset_join_v2 - replaced by bitset_join_v3 (file deleted)
pub mod adaptive;
pub mod result;
pub mod having;
pub mod window;
pub mod shared_execution;
pub mod session;
pub mod dictionary_execution;
pub mod cte_scan;
pub mod derived_table_scan;
pub mod subquery_executor;
pub mod type_conversion;
pub mod transaction;
pub mod lockfree_transaction;
pub mod parallel_executor;
pub mod wal;
pub mod column_identity;
// DEPRECATED: bitset_join (v1) - replaced by bitset_join_v3 (file deleted)
// Note: operators already declared at line 3
pub mod kernels;

// Bitset Engine V3 operators
#[path = "operators/bitset_filter.rs"]
pub mod bitset_filter_v3;
#[path = "operators/bitset_join_v3.rs"]
pub mod bitset_join_v3;
pub mod hash_table;
pub mod operator_contracts;
pub mod pipeline;
pub mod bloom_filter;
pub mod prebuild_joins;
pub mod vectorized;
pub mod wasm_runner;
pub mod approx;
pub mod exec_node;
pub mod panic_boundary;
pub mod robustness;
pub mod monitoring;
pub mod bitset_simd;
pub mod memory_optimized;
pub mod distributed;

pub use engine::*;
pub use operators::*;
pub use batch::*;
pub use wcoj::*;
pub use simd_kernels::*;
pub use adaptive::*;
pub use result::*;
pub use window::*;
pub use shared_execution::*;
pub use session::*;
pub use dictionary_execution::*;
pub use cte_scan::*;
pub use subquery_executor::*;
pub use type_conversion::*;
pub use transaction::*;
pub use wal::*;
pub use column_identity::*;
// DEPRECATED: bitset_join v1 exports
// pub use bitset_join::*;
pub use kernels::*;
pub use vectorized::*;
pub use wasm_runner::*;
pub use exec_node::*;

