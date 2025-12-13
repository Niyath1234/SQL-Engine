//! WorldState-Aware Cache - Cache keys include WorldState hashes for correctness

use crate::worldstate::WorldState;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use ahash::AHasher;

/// Enhanced cache key that includes WorldState versioning
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorldStateCacheKey {
    /// Base query signature (normalized SQL)
    pub query_signature: u64,
    
    /// Global WorldState hash (changes when any metadata changes)
    pub world_hash_global: u64,
    
    /// Relevant WorldState hash (only for tables/edges used in query)
    pub world_hash_relevant: u64,
    
    /// Policy hash (RBAC, SQL policies, query policies)
    pub policy_hash: u64,
}

impl WorldStateCacheKey {
    /// Hash policy registry
    fn hash_policy_registry(registry: &crate::worldstate::policies::PolicyRegistry) -> u64 {
        let mut hasher = AHasher::default();
        registry.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Create a cache key from query signature and WorldState
    pub fn new(
        query_signature: u64,
        world_state: &WorldState,
        relevant_tables: &[String],
        relevant_edges: &[String],
    ) -> Self {
        Self {
            query_signature,
            world_hash_global: world_state.world_hash_global(),
            world_hash_relevant: world_state.world_hash_relevant(relevant_tables, relevant_edges),
            policy_hash: Self::hash_policy_registry(&world_state.policy_registry),
        }
    }
    
    /// Check if cache key is still valid (WorldState hasn't changed)
    pub fn is_valid(
        &self,
        world_state: &WorldState,
        relevant_tables: &[String],
        relevant_edges: &[String],
    ) -> bool {
        self.world_hash_global == world_state.world_hash_global() &&
        self.world_hash_relevant == world_state.world_hash_relevant(relevant_tables, relevant_edges) &&
        self.policy_hash == Self::hash_policy_registry(&world_state.policy_registry)
    }
}

/// Cache invalidation manager
pub struct CacheInvalidationManager;

impl CacheInvalidationManager {
    pub fn new() -> Self {
        Self
    }
    
    /// Invalidate caches affected by table changes
    pub fn invalidate_table_caches(
        plan_cache: &mut crate::query::cache::PlanCache,
        result_cache: &mut crate::cache::ResultCache,
        table_name: &str,
    ) {
        // For now, clear all caches (conservative approach)
        // In production, would track which queries use which tables
        plan_cache.clear();
        result_cache.clear();
    }
    
    /// Invalidate caches affected by rule changes
    pub fn invalidate_rule_caches(
        plan_cache: &mut crate::query::cache::PlanCache,
        result_cache: &mut crate::cache::ResultCache,
    ) {
        // Rule changes affect join plans - clear plan cache
        plan_cache.clear();
        // Result cache might be affected if join results changed
        result_cache.clear();
    }
    
    /// Invalidate caches affected by stats changes
    pub fn invalidate_stats_caches(
        plan_cache: &mut crate::query::cache::PlanCache,
        result_cache: &mut crate::cache::ResultCache,
    ) {
        // Stats changes affect plan selection (join order, etc.)
        plan_cache.clear();
        // Result cache is typically not affected by stats changes
        // (results are the same, just planning is different)
    }
}

impl Default for CacheInvalidationManager {
    fn default() -> Self {
        Self::new()
    }
}

