// Sparse index module for time-series data optimization
// Provides efficient range scans on sorted data with sampling
pub mod builder;
pub mod reader;

pub use builder::*;
pub use reader::*;

