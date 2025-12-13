//! # Hypergraph SQL Engine
//! 
//! A high-performance SQL query engine built on hypergraph data structures.
//! 
//! ## Quick Start
//! 
//! ```rust,no_run
//! use hypergraph_sql_engine::HypergraphSQLEngine;
//! 
//! // Create a new engine instance
//! let mut engine = HypergraphSQLEngine::new();
//! 
//! // Create a table
//! engine.execute_query(
//!     "CREATE TABLE employees (id INT, name VARCHAR, salary FLOAT)"
//! ).unwrap();
//! 
//! // Insert data
//! engine.execute_query(
//!     "INSERT INTO employees VALUES (1, 'Alice', 75000.0)"
//! ).unwrap();
//! 
//! // Query data
//! let result = engine.execute_query(
//!     "SELECT name, salary FROM employees WHERE salary > 70000"
//! ).unwrap();
//! 
//! println!("Found {} rows", result.row_count);
//! ```
//! 
//! ## Features
//! 
//! - **Easy to Medium SQL Support**: SELECT, WHERE, JOIN, GROUP BY, ORDER BY
//! - **High Performance**: SIMD-optimized execution, columnar storage
//! - **Memory Efficient**: Multi-tier memory management
//! - **Query Caching**: Plan and result caching for faster repeated queries

// Internal modules
pub mod storage;
pub mod hypergraph;
pub mod query;
pub mod execution;
pub mod cache;
pub mod engine;
pub mod web;
pub mod result_format;
pub mod learning;
pub mod spill;
pub mod index;
pub mod codegen;
pub mod metadata;
pub mod error;
pub mod config;
pub mod worldstate;
pub mod ingestion;
pub mod llm;

// Public API - Main types users need
pub use engine::HypergraphSQLEngine;
pub use engine::QueryResult;
pub use engine::CacheStats;
pub use engine::QueryClass;
pub use engine::LlmQueryMode;
pub use engine::LlmQueryRequest;

// Re-export commonly used error types
pub use error::EngineError;

// Re-export execution result types for advanced usage
pub use execution::engine::QueryResult as ExecutionQueryResult;
pub use execution::batch::ExecutionBatch;
