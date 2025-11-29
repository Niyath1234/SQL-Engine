/// Cross-Query Optimization (Shared Scans & Joins)
/// Share intermediate results between concurrent or recent queries
use crate::query::fingerprint::QueryFingerprint as Fingerprint;
use crate::query::plan::PlanOperator;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Shared execution result
#[derive(Clone, Debug)]
pub struct SharedResult {
    pub fingerprint: Fingerprint,
    pub result_type: SharedResultType,
    pub data: Vec<u8>, // Serialized result data
    pub created_at: Instant,
    pub last_used_at: Instant,
    pub use_count: usize,
    pub pinned: bool,
}

/// Type of shared result
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SharedResultType {
    /// Shared scan result (table + predicate)
    SharedScan { table: String, predicate_hash: u64 },
    /// Shared hash table (join key + build side)
    SharedHashTable { join_key: String, build_side: String },
    /// Shared partial aggregate (grouping keys)
    SharedPartialAggregate { group_keys: Vec<String> },
}

/// Global shared execution manager
pub struct SharedExecutionManager {
    /// Map from fingerprint to shared result
    shared_results: Arc<Mutex<HashMap<Fingerprint, SharedResult>>>,
    /// Maximum number of shared results to keep
    max_results: usize,
    /// Time-to-live for shared results
    ttl: Duration,
}

impl SharedExecutionManager {
    pub fn new(max_results: usize, ttl_seconds: u64) -> Self {
        Self {
            shared_results: Arc::new(Mutex::new(HashMap::new())),
            max_results,
            ttl: Duration::from_secs(ttl_seconds),
        }
    }
    
    /// Find shared execution opportunities for a query
    pub fn find_shared_opportunities(&self, fingerprint: &Fingerprint) -> SharedPlanHints {
        let results = self.shared_results.lock().unwrap();
        
        // Look for matching fingerprints
        if let Some(shared_result) = results.get(fingerprint) {
            // Check if result is still valid (not expired)
            if shared_result.created_at.elapsed() < self.ttl {
                tracing::debug!(
                    "Found shared execution opportunity: {:?} (use_count={})",
                    shared_result.result_type,
                    shared_result.use_count
                );
                
                // Update last used time
                // Note: In full implementation, we'd update the actual result
                
                return SharedPlanHints {
                    reuse_scan: matches!(shared_result.result_type, SharedResultType::SharedScan { .. }),
                    reuse_hash_table: matches!(shared_result.result_type, SharedResultType::SharedHashTable { .. }),
                    reuse_aggregate: matches!(shared_result.result_type, SharedResultType::SharedPartialAggregate { .. }),
                    shared_fingerprint: Some(fingerprint.clone()),
                };
            }
        }
        
        // No shared opportunity found
        SharedPlanHints::default()
    }
    
    /// Register a shared result
    pub fn register_shared_result(&self, fingerprint: Fingerprint, result_type: SharedResultType, data: Vec<u8>) {
        let mut results = self.shared_results.lock().unwrap();
        
        // Evict old results if at capacity
        if results.len() >= self.max_results {
            self.evict_oldest(&mut results);
        }
        
        let shared_result = SharedResult {
            fingerprint: fingerprint.clone(),
            result_type,
            data,
            created_at: Instant::now(),
            last_used_at: Instant::now(),
            use_count: 0,
            pinned: false,
        };
        
        results.insert(fingerprint, shared_result);
        tracing::debug!("Registered shared execution result");
    }
    
    /// Pin a shared result (prevent eviction)
    pub fn pin_result(&self, fingerprint: &Fingerprint) {
        let mut results = self.shared_results.lock().unwrap();
        if let Some(result) = results.get_mut(fingerprint) {
            result.pinned = true;
        }
    }
    
    /// Unpin a shared result
    pub fn unpin_result(&self, fingerprint: &Fingerprint) {
        let mut results = self.shared_results.lock().unwrap();
        if let Some(result) = results.get_mut(fingerprint) {
            result.pinned = false;
        }
    }
    
    /// Evict oldest unused results
    fn evict_oldest(&self, results: &mut HashMap<Fingerprint, SharedResult>) {
        // Find oldest unpinned result
        let oldest = results
            .iter()
            .filter(|(_, r)| !r.pinned)
            .min_by_key(|(_, r)| r.last_used_at);
        
        if let Some((fingerprint, _)) = oldest {
            let fingerprint = fingerprint.clone();
            results.remove(&fingerprint);
            tracing::debug!("Evicted shared result: {:?}", fingerprint.hash);
        }
    }
    
    /// Clean up expired results
    pub fn cleanup_expired(&self) {
        let mut results = self.shared_results.lock().unwrap();
        let now = Instant::now();
        
        results.retain(|_, result| {
            result.pinned || now.duration_since(result.created_at) < self.ttl
        });
    }
}

/// Hints for shared execution opportunities
#[derive(Clone, Debug, Default)]
pub struct SharedPlanHints {
    pub reuse_scan: bool,
    pub reuse_hash_table: bool,
    pub reuse_aggregate: bool,
    pub shared_fingerprint: Option<Fingerprint>,
}

impl SharedPlanHints {
    pub fn has_opportunities(&self) -> bool {
        self.reuse_scan || self.reuse_hash_table || self.reuse_aggregate
    }
}

