/// Metadata module for stable column identity and schema management
/// 
/// This module provides DuckDB/Presto-grade column identity and schema resolution
/// for robust join execution with 50-100+ tables.

pub mod column_id;
pub mod schema;

pub use column_id::{ColumnId, ColumnOrdinal};
pub use schema::{ColumnSchema, SchemaRef};

