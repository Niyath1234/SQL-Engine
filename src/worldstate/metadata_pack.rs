//! Metadata Pack - RBAC-filtered metadata for LLM/planner consumption

use serde::{Deserialize, Serialize};
use crate::worldstate::{WorldState, SchemaRegistry, JoinRuleRegistry, StatsRegistry};
use crate::worldstate::schema::{TableSchema, ColumnInfo};
use crate::worldstate::rules::JoinRule;
use crate::worldstate::stats::TableStats;

/// Simplified table info for metadata pack
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub version: u64,
}

/// Simplified join rule info for metadata pack
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRuleInfo {
    pub id: String,
    pub left_table: String,
    pub left_key: Vec<String>,
    pub right_table: String,
    pub right_key: Vec<String>,
    pub join_type: String,
    pub cardinality: String,
    pub justification: Option<String>,
}

/// Simplified filter rule info for metadata pack
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterRuleInfo {
    pub id: String,
    pub table_name: String,
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
    pub mandatory: bool,
    pub justification: Option<String>,
}

/// Hypergraph adjacency info - shows which tables are directly connected
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HypergraphEdge {
    pub from_table: String,
    pub to_table: String,
    pub via_column: String, // The foreign key column name
}

/// Shortest path between two tables in the hypergraph
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TablePath {
    pub from_table: String,
    pub to_table: String,
    pub path: Vec<String>, // Intermediate tables in order, e.g., ["orders", "customers"]
    pub joins_needed: Vec<JoinRuleInfo>, // The exact join rules to follow this path
}

/// Simplified stats for metadata pack
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatsInfo {
    pub table_name: String,
    pub row_count: u64,
    pub column_stats: Vec<ColumnStatInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnStatInfo {
    pub column: String,
    pub ndv: Option<u64>,
    pub null_rate: Option<f64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

/// Sample values for grounding (up to 10 per column)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleValues {
    pub column: String,
    pub values: Vec<String>,
}

/// Metadata Pack - RBAC-filtered, ready for LLM consumption
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataPack {
    /// Tables (filtered by RBAC)
    pub tables: Vec<TableInfo>,
    
    /// Join rules (only approved)
    pub join_rules: Vec<JoinRuleInfo>,
    
    /// Filter rules (business rules - only approved mandatory ones)
    pub filter_rules: Vec<FilterRuleInfo>,
    
    /// Hypergraph structure - direct edges between tables
    pub hypergraph_edges: Vec<HypergraphEdge>,
    
    /// Statistics (filtered by RBAC)
    pub stats: Vec<StatsInfo>,
    
    /// Sample values (for grounding)
    pub sample_values: Vec<SampleValues>,
    
    /// World hash (for cache keying)
    pub world_hash: u64,
    
    /// Timestamp when pack was generated
    pub generated_at: u64,
}

/// Metadata Pack Builder
pub struct MetadataPackBuilder<'a> {
    world_state: &'a WorldState,
    user_id: Option<String>,
    include_samples: bool,
    max_samples_per_column: usize,
}

impl<'a> MetadataPackBuilder<'a> {
    pub fn new(world_state: &'a WorldState) -> Self {
        Self {
            world_state,
            user_id: None,
            include_samples: true,
            max_samples_per_column: 10,
        }
    }
    
    /// Apply RBAC filtering
    pub fn with_rbac_filter(mut self, user_id: Option<&str>) -> Self {
        self.user_id = user_id.map(|s| s.to_string());
        self
    }
    
    /// Include/exclude sample values
    pub fn with_samples(mut self, include: bool) -> Self {
        self.include_samples = include;
        self
    }
    
    /// Build the metadata pack
    pub fn build(&self) -> MetadataPack {
        let world_hash = self.world_state.world_hash_global();
        
        // Get RBAC policy
        let rbac_policy = self.world_state.policy_registry.get_rbac_policy(
            self.user_id.as_deref()
        );
        
        // Filter tables by RBAC
        let tables: Vec<TableInfo> = self.world_state.schema_registry
            .list_tables()
            .iter()
            .filter(|table_name| {
                // Check if table is allowed
                if let Some(policy) = rbac_policy {
                    if !policy.allowed_tables.is_empty() {
                        if !policy.allowed_tables.contains(*table_name) {
                            return false;
                        }
                    }
                    if policy.denied_tables.contains(*table_name) {
                        return false;
                    }
                }
                true
            })
            .filter_map(|table_name| {
                self.world_state.schema_registry.get_table(table_name)
                    .map(|schema| {
                        // Filter columns by RBAC
                        let columns: Vec<ColumnInfo> = if let Some(policy) = rbac_policy {
                            if let Some(allowed_cols) = policy.allowed_columns.get(table_name) {
                                schema.columns.iter()
                                    .filter(|col| allowed_cols.contains(&col.name))
                                    .cloned()
                                    .collect()
                            } else {
                                schema.columns.clone()
                            }
                        } else {
                            schema.columns.clone()
                        };
                        
                        TableInfo {
                            name: schema.table_name.clone(),
                            columns,
                            version: schema.version,
                        }
                    })
            })
            .collect();
        
        // Get approved join rules
        let join_rules: Vec<JoinRuleInfo> = self.world_state.rule_registry
            .list_approved_rules()
            .iter()
            .map(|rule| JoinRuleInfo {
                id: rule.id.clone(),
                left_table: rule.left_table.clone(),
                left_key: rule.left_key.clone(),
                right_table: rule.right_table.clone(),
                right_key: rule.right_key.clone(),
                join_type: rule.join_type.clone(),
                cardinality: rule.cardinality.clone(),
                justification: rule.justification.clone(),
            })
            .collect();
        
        // Get approved mandatory filter rules (business rules)
        let filter_rules: Vec<FilterRuleInfo> = self.world_state.filter_rule_registry
            .list_approved_rules()
            .iter()
            .filter(|rule| rule.mandatory)  // Only mandatory rules are auto-applied
            .map(|rule| FilterRuleInfo {
                id: rule.id.clone(),
                table_name: rule.table_name.clone(),
                column: rule.column.clone(),
                operator: rule.operator.clone(),
                value: rule.value.clone(),
                mandatory: rule.mandatory,
                justification: rule.justification.clone(),
            })
            .collect();
        
        // Build hypergraph edges from join rules (for graph traversal guidance)
        let hypergraph_edges: Vec<HypergraphEdge> = join_rules.iter()
            .filter_map(|rule| {
                rule.left_key.first().map(|left_key| HypergraphEdge {
                    from_table: rule.left_table.clone(),
                    to_table: rule.right_table.clone(),
                    via_column: left_key.clone(),
                })
            })
            .collect();
        
        // Get stats (filtered by RBAC)
        let stats: Vec<StatsInfo> = tables.iter()
            .filter_map(|table_info| {
                self.world_state.stats_registry.get_table_stats(&table_info.name)
                    .map(|table_stats| {
                        StatsInfo {
                            table_name: table_stats.table_name.clone(),
                            row_count: table_stats.row_count,
                            column_stats: table_stats.column_stats.values()
                                .map(|col_stat| ColumnStatInfo {
                                    column: col_stat.column.clone(),
                                    ndv: col_stat.ndv,
                                    null_rate: col_stat.null_rate,
                                    min_value: col_stat.min_value.clone(),
                                    max_value: col_stat.max_value.clone(),
                                })
                                .collect(),
                        }
                    })
            })
            .collect();
        
        // Sample values (placeholder - would need actual data access)
        let sample_values = if self.include_samples {
            Vec::new() // TODO: Implement sample value extraction from actual data
        } else {
            Vec::new()
        };
        
        MetadataPack {
            tables,
            join_rules,
            filter_rules,
            hypergraph_edges,
            stats,
            sample_values,
            world_hash,
            generated_at: Self::now_timestamp(),
        }
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

