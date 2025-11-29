// Micro-kernel registry for specialized aggregation and filtering operations
// These kernels are monomorphized for specific types to avoid runtime dispatch overhead

pub mod agg_sum;
pub mod agg_avg;
pub mod filter_fast;

pub use agg_sum::*;
pub use agg_avg::*;
pub use filter_fast::*;

