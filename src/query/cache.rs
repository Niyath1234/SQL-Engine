use crate::query::plan::QueryPlan;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use ahash::AHasher;

/// Cache for query plans
pub struct PlanCache {
    /// Map from query signature to plan
    plans: HashMap<QuerySignature, QueryPlan>,
    
    /// Maximum number of plans to cache
    max_size: usize,
}

#[derive(Clone, Debug)]
pub struct QuerySignature {
    /// Normalized SQL query string
    pub sql: String,
    
    /// Hash of the query
    pub hash: u64,
}

impl QuerySignature {
    pub fn from_sql(sql: &str) -> Self {
        let normalized = normalize_sql(sql);
        let hash = hash_query(&normalized);
        
        Self {
            sql: normalized,
            hash,
        }
    }
    
    pub fn hash(&self) -> u64 {
        self.hash
    }
}

impl PartialEq for QuerySignature {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for QuerySignature {}

impl Hash for QuerySignature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl PlanCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            plans: HashMap::new(),
            max_size,
        }
    }
    
    /// Get a cached plan
    pub fn get(&self, signature: &QuerySignature) -> Option<&QueryPlan> {
        self.plans.get(signature)
    }
    
    /// Insert a plan
    pub fn insert(&mut self, signature: QuerySignature, plan: QueryPlan) {
        if self.plans.len() >= self.max_size {
            // Evict oldest (simple strategy - could use LRU)
            if let Some(key) = self.plans.keys().next().cloned() {
                self.plans.remove(&key);
            }
        }
        self.plans.insert(signature, plan);
    }
    
    /// Check if a similar query exists (for reuse)
    pub fn find_similar(&self, target: &QuerySignature) -> Option<&QueryPlan> {
        // Simple exact match for now
        // TODO: Implement similarity detection
        self.plans.values().next()
    }
    
    /// Clear all cached plans
    pub fn clear(&mut self) {
        self.plans.clear();
    }
    
    /// Get cache size
    pub fn len(&self) -> usize {
        self.plans.len()
    }
}

/// Normalize SQL query for caching
fn normalize_sql(sql: &str) -> String {
    // Remove extra whitespace
    let normalized = sql
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    
    // TODO: Normalize further (e.g., lowercase keywords, remove comments)
    normalized.to_lowercase()
}

/// Hash a query string
fn hash_query(query: &str) -> u64 {
    let mut hasher = AHasher::default();
    query.hash(&mut hasher);
    hasher.finish()
}

