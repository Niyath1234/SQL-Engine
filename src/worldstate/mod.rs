//! WorldState - The authoritative "spine" of DA_Cursor
//! 
//! WorldState is the single source of truth for:
//! - Schema Registry (tables, columns, types, versions)
//! - Keys (primary keys, natural keys, event time)
//! - Rules (authoritative join rules)
//! - Hypergraph Edges (join relationships)
//! - Stats (row counts, NDV, null rates, distributions)
//! - Lineage (source → table → schema version)
//! - Policies/RBAC (allowed tables/columns, SQL verbs, limits)

pub mod schema;
pub mod keys;
pub mod rules;
pub mod stats;
pub mod lineage;
pub mod policies;
pub mod metadata_pack;
pub mod persistence;
pub mod stats_collector;
pub mod cost_model;

pub use schema::{SchemaRegistry, TableSchema, ColumnInfo, SchemaVersion};
pub use keys::{KeyRegistry, PrimaryKey, NaturalKey, EventTime, DedupeStrategy};
pub use rules::{JoinRule, JoinRuleRegistry, FilterRule, FilterRuleRegistry, RuleState};
pub use stats::{StatsRegistry, ColumnStats, TableStats};
pub use lineage::{LineageRegistry, SourceInfo, IngestionRun, TableLineage};
pub use policies::{PolicyRegistry, RBACPolicy, SQLPolicy, QueryPolicy};
pub use metadata_pack::{MetadataPack, MetadataPackBuilder};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::RwLock;

/// The authoritative WorldState - the "spine" of DA_Cursor
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorldState {
    /// Schema registry: table → schema version → columns/types
    pub schema_registry: SchemaRegistry,
    
    /// Key registry: primary keys, natural keys, event time columns
    pub key_registry: KeyRegistry,
    
    /// Join rules: authoritative join relationships
    pub rule_registry: JoinRuleRegistry,
    
    /// Filter rules: business rules for default filters
    pub filter_rule_registry: FilterRuleRegistry,
    
    /// Statistics: row counts, NDV, null rates, distributions
    pub stats_registry: StatsRegistry,
    
    /// Lineage: source → ingestion → table → schema version
    pub lineage_registry: LineageRegistry,
    
    /// Policies: RBAC, SQL allowed verbs, query limits
    pub policy_registry: PolicyRegistry,
    
    /// Global version counter (increments on any change)
    pub version: u64,
    
    /// Timestamp of last update
    pub last_updated: u64,
}

impl WorldState {
    /// Create a new empty WorldState
    pub fn new() -> Self {
        Self {
            schema_registry: SchemaRegistry::new(),
            key_registry: KeyRegistry::new(),
            rule_registry: JoinRuleRegistry::new(),
            filter_rule_registry: FilterRuleRegistry::new(),
            stats_registry: StatsRegistry::new(),
            lineage_registry: LineageRegistry::new(),
            policy_registry: PolicyRegistry::new(),
            version: 1,
            last_updated: Self::now_timestamp(),
        }
    }
    
    /// Compute global world hash (changes when ANY part of world changes)
    pub fn world_hash_global(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        self.version.hash(&mut hasher);
        self.schema_registry.hash(&mut hasher);
        self.key_registry.hash(&mut hasher);
        self.rule_registry.hash(&mut hasher);
        // Note: stats may change frequently, so we may want to exclude them from global hash
        // or use a separate "schema hash" vs "data hash"
        hasher.finish()
    }
    
    /// Compute relevant world hash for specific tables/edges
    /// Used for cache keying: only invalidate when relevant parts change
    pub fn world_hash_relevant(&self, tables: &[String], edges: &[String]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        
        // Hash relevant table schemas
        for table in tables {
            if let Some(schema) = self.schema_registry.get_table(table) {
                schema.hash(&mut hasher);
            }
        }
        
        // Hash relevant join rules/edges
        for edge_id in edges {
            if let Some(rule) = self.rule_registry.get_rule(edge_id) {
                rule.hash(&mut hasher);
            }
        }
        
        hasher.finish()
    }
    
    /// Increment version (call after any mutation)
    pub fn bump_version(&mut self) {
        self.version += 1;
        self.last_updated = Self::now_timestamp();
    }
    
    /// Get current timestamp (Unix epoch seconds)
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
    
    /// Export metadata pack for a user (RBAC-filtered)
    pub fn build_metadata_pack(&self, user_id: Option<&str>) -> MetadataPack {
        MetadataPackBuilder::new(self)
            .with_rbac_filter(user_id)
            .build()
    }
}

impl Default for WorldState {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe wrapper for WorldState
pub type WorldStateRef = Arc<RwLock<WorldState>>;

/// Helper to create a WorldStateRef
pub fn new_world_state_ref() -> WorldStateRef {
    Arc::new(RwLock::new(WorldState::new()))
}

