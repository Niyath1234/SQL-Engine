/// Automatic Workload-Level Plan Caching
/// Caches plans at workload level (not just query level) for better reuse
use crate::query::fingerprint::QueryFingerprint;
use crate::query::plan::QueryPlan;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Workload pattern (sequence of query fingerprints)
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct WorkloadPattern {
    pub fingerprints: Vec<u64>, // Sequence of query fingerprint hashes
    pub frequency: usize,
}

/// Cached workload plan
#[derive(Clone, Debug)]
pub struct CachedWorkloadPlan {
    pub pattern: WorkloadPattern,
    pub plans: Vec<QueryPlan>, // Plans for each query in the pattern
    pub created_at: Instant,
    pub last_used_at: Instant,
    pub use_count: usize,
}

/// Workload-level plan cache
pub struct WorkloadPlanCache {
    /// Cache of workload patterns to plans
    cache: Arc<Mutex<HashMap<WorkloadPattern, CachedWorkloadPlan>>>,
    /// Recent query sequence (for pattern detection)
    recent_queries: Arc<Mutex<VecDeque<QueryFingerprint>>>,
    /// Maximum sequence length to track
    max_sequence_length: usize,
    /// Maximum cache size
    max_cache_size: usize,
    /// TTL for cached plans
    ttl: Duration,
}

impl WorkloadPlanCache {
    pub fn new(max_cache_size: usize, ttl_seconds: u64, max_sequence_length: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            recent_queries: Arc::new(Mutex::new(VecDeque::new())),
            max_sequence_length,
            max_cache_size,
            ttl: Duration::from_secs(ttl_seconds),
        }
    }
    
    /// Record a query in the sequence
    pub fn record_query(&self, fingerprint: &QueryFingerprint) {
        let mut recent = self.recent_queries.lock().unwrap();
        recent.push_back(fingerprint.clone());
        
        // Keep only recent queries
        while recent.len() > self.max_sequence_length {
            recent.pop_front();
        }
    }
    
    /// Detect workload patterns from recent query sequence
    fn detect_patterns(&self) -> Vec<WorkloadPattern> {
        let recent = self.recent_queries.lock().unwrap();
        let mut patterns = HashMap::new();
        
        // Look for sequences of length 2-5
        for window_size in 2..=5.min(recent.len()) {
            for i in 0..=recent.len().saturating_sub(window_size) {
                let sequence: Vec<u64> = recent.iter()
                    .skip(i)
                    .take(window_size)
                    .map(|fp| fp.hash)
                    .collect();
                
                *patterns.entry(WorkloadPattern {
                    fingerprints: sequence,
                    frequency: 0,
                }).or_insert(0) += 1;
            }
        }
        
        // Convert to patterns with frequency
        patterns.into_iter()
            .map(|(mut pattern, freq)| {
                pattern.frequency = freq;
                pattern
            })
            .filter(|p| p.frequency >= 3) // At least 3 occurrences
            .collect()
    }
    
    /// Get cached plans for a workload pattern
    pub fn get_cached_plans(&self, pattern: &WorkloadPattern) -> Option<Vec<QueryPlan>> {
        let cache = self.cache.lock().unwrap();
        
        if let Some(cached) = cache.get(pattern) {
            // Check TTL
            if cached.last_used_at.elapsed() < self.ttl {
                // Update last used time
                drop(cache);
                let mut cache = self.cache.lock().unwrap();
                if let Some(cached) = cache.get_mut(pattern) {
                    cached.last_used_at = Instant::now();
                    cached.use_count += 1;
                    return Some(cached.plans.clone());
                }
            }
        }
        
        None
    }
    
    /// Cache plans for a workload pattern
    pub fn cache_plans(&self, pattern: WorkloadPattern, plans: Vec<QueryPlan>) {
        let mut cache = self.cache.lock().unwrap();
        
        // Evict if at capacity
        if cache.len() >= self.max_cache_size {
            self.evict_oldest(&mut cache);
        }
        
        let cached = CachedWorkloadPlan {
            pattern: pattern.clone(),
            plans,
            created_at: Instant::now(),
            last_used_at: Instant::now(),
            use_count: 0,
        };
        
        cache.insert(pattern, cached);
    }
    
    /// Auto-detect and cache workload patterns
    pub fn auto_cache_patterns(&self, plan_provider: impl Fn(&QueryFingerprint) -> Option<QueryPlan>) {
        let patterns = self.detect_patterns();
        
        for pattern in patterns {
            // Check if already cached
            if self.get_cached_plans(&pattern).is_none() {
                // Try to get plans for each query in pattern
                let mut plans = Vec::new();
                let recent = self.recent_queries.lock().unwrap();
                
                for &hash in &pattern.fingerprints {
                    if let Some(fingerprint) = recent.iter().find(|fp| fp.hash == hash) {
                        if let Some(plan) = plan_provider(fingerprint) {
                            plans.push(plan);
                        } else {
                            break; // Can't build full pattern
                        }
                    } else {
                        break; // Fingerprint not in recent history
                    }
                }
                
                if plans.len() == pattern.fingerprints.len() {
                    // Successfully built all plans
                    self.cache_plans(pattern, plans);
                }
            }
        }
    }
    
    /// Evict oldest unused entries
    fn evict_oldest(&self, cache: &mut HashMap<WorkloadPattern, CachedWorkloadPlan>) {
        let oldest = cache.iter()
            .min_by_key(|(_, cached)| cached.last_used_at);
        
        if let Some((pattern, _)) = oldest {
            let pattern = pattern.clone();
            cache.remove(&pattern);
        }
    }
    
    /// Clean up expired entries
    pub fn cleanup_expired(&self) {
        let mut cache = self.cache.lock().unwrap();
        let now = Instant::now();
        
        cache.retain(|_, cached| now.duration_since(cached.created_at) < self.ttl);
    }
}

