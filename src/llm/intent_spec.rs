/// IntentSpec: Schema-agnostic query intent specification
/// 
/// This is what the LLM outputs - generic "what to compute" bullets, NOT "how to join".
/// The deterministic compiler (IntentCompiler) uses the hypergraph spine to resolve
/// everything into actual tables, columns, and joins.
/// 
/// **Determinism Rule**: Same IntentSpec + same MetadataPack(world_hash) → same SQL
/// 
/// **Hard Constraints**:
/// - LLM must NOT output table names, column names, or join keys
/// - If LLM outputs schema references, they go into `ambiguities` and block compilation
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentSpec {
    /// Task type: "aggregate", "list", "count", "exists", "compare"
    pub task: String,
    
    /// Metrics to compute (for aggregate tasks)
    /// Contains: operation (sum, avg, count, etc.), expression hints (column names), semantic hints
    pub metrics: Vec<MetricSpec>,
    
    /// Grain/dimensions: what to group by or list
    /// Contains: entity hints (table names if user mentioned them), attribute hints (column names)
    pub grain: Vec<GrainSpec>,
    
    /// Filters to apply
    /// Contains: field hints (column names), operations (eq, gt, etc.), values, semantic hints
    pub filters: Vec<FilterSpec>,
    
    /// Explicit table names mentioned by user (if any)
    /// This helps the deterministic compiler prioritize these tables
    pub table_hints: Option<Vec<String>>,
    
    /// Time window (if applicable)
    pub time: Option<TimeWindowSpec>,
    
    /// Sorting
    pub sort: Vec<SortSpec>,
    
    /// Limit (max rows)
    pub limit: Option<u32>,
    
    /// Ambiguities: things LLM couldn't resolve (blocks compilation in strict mode)
    pub ambiguities: Vec<AmbiguitySpec>,
    
    /// Confidence: overall confidence (0.0-1.0)
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSpec {
    /// Metric name (e.g., "total_sales", "order_count")
    pub name: String,
    
    /// Operation: "sum", "count", "avg", "min", "max", "distinct_count"
    pub op: String,
    
    /// Semantic hint: "money", "count", "percentage", "date", "text"
    pub semantic: Option<String>,
    
    /// Expression hint (if metric needs a specific field, e.g., "amount" for sum)
    pub expression_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainSpec {
    /// Entity: "customer", "product", "day", "category"
    pub entity: String,
    
    /// Attribute hint: "name", "id", "email" (optional, compiler will choose best match)
    pub attribute_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    /// Field hint: "status", "date", "amount" (semantic, not actual column)
    pub field_hint: String,
    
    /// Operation: "eq", "ne", "gt", "gte", "lt", "lte", "in", "like", "between"
    pub op: String,
    
    /// Value (JSON value: string, number, array, etc.)
    pub value: serde_json::Value,
    
    /// Semantic hint: "status", "date", "money", "text"
    pub semantic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindowSpec {
    /// Type: "relative" or "absolute"
    pub r#type: String,
    
    /// For relative: "last_7_days", "last_month", "last_year", "this_month", "this_year"
    /// For absolute: ISO 8601 date range
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    /// Sort by: metric name or grain entity
    pub by: String,
    
    /// Direction: "asc" or "desc"
    pub dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguitySpec {
    /// What was ambiguous
    pub what: String,
    
    /// Possible interpretations
    pub interpretations: Vec<String>,
    
    /// Why it's ambiguous
    pub reason: String,
}

impl IntentSpec {
    /// Validate that IntentSpec doesn't contain schema references (table/column names)
    /// Returns errors if found (should block compilation in strict mode)
    pub fn validate_no_schema_references(&self) -> Vec<String> {
        let mut errors = Vec::new();
        
        // Check metrics
        for metric in &self.metrics {
            if metric.name.contains('.') {
                errors.push(format!("Metric '{}' contains schema reference (table.column)", metric.name));
            }
            if let Some(ref expr) = metric.expression_hint {
                if expr.contains('.') {
                    errors.push(format!("Metric expression_hint '{}' contains schema reference", expr));
                }
            }
        }
        
        // Check grain
        for grain in &self.grain {
            if grain.entity.contains('.') {
                errors.push(format!("Grain entity '{}' contains schema reference", grain.entity));
            }
            if let Some(ref attr) = grain.attribute_hint {
                if attr.contains('.') {
                    errors.push(format!("Grain attribute_hint '{}' contains schema reference", attr));
                }
            }
        }
        
        // Check filters
        for filter in &self.filters {
            if filter.field_hint.contains('.') {
                errors.push(format!("Filter field_hint '{}' contains schema reference", filter.field_hint));
            }
        }
        
        errors
    }
    
    /// Check if IntentSpec has blocking ambiguities (strict mode)
    pub fn has_blocking_ambiguities(&self) -> bool {
        !self.ambiguities.is_empty() || self.confidence < 0.7
    }
}


