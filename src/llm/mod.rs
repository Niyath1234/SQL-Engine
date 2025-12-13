//! LLM Module - IntentSpec-based deterministic query pipeline

pub mod ollama_client;
pub mod plan_generator; // Still used for StructuredPlan types
pub mod validators;
pub mod audit_log;
pub mod sql_generator;
pub mod plan_normalizer;
pub mod intent_spec;
pub mod intent_spec_generator;
pub mod intent_compiler;

// DEPRECATED: llm_output_corrector - No longer needed with IntentSpec compiler
// The compiler handles all corrections deterministically using hypergraph

pub use ollama_client::OllamaClient;
pub use plan_generator::{PlanGenerator, StructuredPlan}; // PlanGenerator deprecated, kept for old test files only
pub use validators::{PlanValidator, ValidationResult};
pub use audit_log::AuditLog;
pub use sql_generator::SQLGenerator;
pub use plan_normalizer::PlanNormalizer;
pub use intent_spec::IntentSpec;
pub use intent_spec_generator::IntentSpecGenerator;
pub use intent_compiler::IntentCompiler;

