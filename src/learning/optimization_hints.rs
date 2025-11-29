/// Optimization hints generated from learned patterns
use crate::query::ParsedQuery;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Optimization hints suggested by the pattern learner
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationHints {
    /// Suggested indexes to create
    pub suggested_indexes: Vec<String>,
    
    /// Suggested join order
    pub suggested_join_order: Option<Vec<String>>,
    
    /// Filter pushdown hints
    pub filter_pushdown_hints: Vec<FilterPattern>,
    
    /// Predicted next query
    pub predicted_next_query: Option<PredictedQuery>,
    
    /// Suggested plan optimizations
    pub plan_hints: HashMap<String, String>,
}

/// Filter pattern for pushdown hints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterPattern {
    pub table: String,
    pub column: String,
    pub operator: String,
    pub value_distribution: Vec<crate::storage::fragment::Value>,
}

/// Predicted next query based on learned patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictedQuery {
    pub likely_tables: Vec<String>,
    pub likely_columns: Vec<String>,
    pub likely_joins: Vec<JoinPattern>,
    pub confidence: f32,
}

/// Join pattern from learned queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinPattern {
    pub left_table: String,
    pub right_table: String,
    pub join_column: String,
    pub selectivity: f64,
    pub frequency: usize,
}

