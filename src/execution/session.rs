use crate::query::cache::{PlanCache, QuerySignature};
use crate::cache::result_cache::ResultCache;
use crate::execution::batch::ExecutionBatch;
use crate::hypergraph::node::NodeId;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Session-aware working set for LLM queries
/// Tracks per-session hot fragments, plan cache, and result cache
pub struct SessionWorkingSet {
    /// Per-session plan cache (keyed by session_id)
    plan_caches: HashMap<String, PlanCache>,
    
    /// Per-session result cache (keyed by session_id)
    result_caches: HashMap<String, ResultCache>,
    
    /// Per-session hot fragment access counts
    /// Maps: session_id -> (node_id, fragment_idx) -> access_count
    hot_fragments: HashMap<String, HashMap<(NodeId, usize), u64>>,
    
    /// Per-session pinned fragments (Phase 2: kept in L1 RAM)
    /// Maps: session_id -> set of (node_id, fragment_idx) that are pinned
    pinned_fragments: HashMap<String, std::collections::HashSet<(NodeId, usize)>>,
    
    /// Per-session last access time (for LRU eviction)
    last_access: HashMap<String, u64>,
    
    /// Maximum number of sessions to keep
    max_sessions: usize,
    
    /// Maximum plans per session
    max_plans_per_session: usize,
    
    /// Maximum results per session
    max_results_per_session: usize,
    
    /// TTL for sessions (seconds of inactivity before eviction)
    session_ttl_seconds: u64,
}

impl SessionWorkingSet {
    pub fn new() -> Self {
        Self {
            plan_caches: HashMap::new(),
            result_caches: HashMap::new(),
            hot_fragments: HashMap::new(),
            pinned_fragments: HashMap::new(),
            last_access: HashMap::new(),
            max_sessions: 100, // Support up to 100 concurrent sessions
            max_plans_per_session: 50, // 50 plans per session
            max_results_per_session: 20, // 20 results per session
            session_ttl_seconds: 3600, // 1 hour of inactivity
        }
    }
    
    /// Get or create plan cache for a session
    pub fn get_plan_cache(&mut self, session_id: &str) -> &mut PlanCache {
        self.update_access_time(session_id);
        
        if !self.plan_caches.contains_key(session_id) {
            self.evict_if_needed();
            self.plan_caches.insert(
                session_id.to_string(),
                PlanCache::new(self.max_plans_per_session),
            );
        }
        
        self.plan_caches.get_mut(session_id).unwrap()
    }
    
    /// Get or create result cache for a session
    pub fn get_result_cache(&mut self, session_id: &str) -> &mut ResultCache {
        self.update_access_time(session_id);
        
        if !self.result_caches.contains_key(session_id) {
            self.evict_if_needed();
            self.result_caches.insert(
                session_id.to_string(),
                ResultCache::new(self.max_results_per_session, 300), // 5 min TTL
            );
        }
        
        self.result_caches.get_mut(session_id).unwrap()
    }
    
    /// Record fragment access for a session
    pub fn record_fragment_access(&mut self, session_id: &str, node_id: NodeId, fragment_idx: usize) {
        self.update_access_time(session_id);
        
        let session_fragments = self.hot_fragments
            .entry(session_id.to_string())
            .or_insert_with(HashMap::new);
        
        *session_fragments.entry((node_id, fragment_idx)).or_insert(0) += 1;
    }
    
    /// Get hot fragments for a session (top N by access count)
    pub fn get_hot_fragments(&self, session_id: &str, top_n: usize) -> Vec<((NodeId, usize), u64)> {
        if let Some(fragments) = self.hot_fragments.get(session_id) {
            let mut sorted: Vec<_> = fragments.iter()
                .map(|(k, v)| (*k, *v))
                .collect();
            sorted.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
            sorted.into_iter().take(top_n).collect()
        } else {
            Vec::new()
        }
    }
    
    /// Pin fragments for a session (Phase 2: keep in L1 RAM)
    /// Pins the top N hot fragments for a session
    pub fn pin_hot_fragments(&mut self, session_id: &str, top_n: usize) {
        self.update_access_time(session_id);
        
        let hot = self.get_hot_fragments(session_id, top_n);
        let pinned = self.pinned_fragments
            .entry(session_id.to_string())
            .or_insert_with(std::collections::HashSet::new);
        
        for ((node_id, fragment_idx), _count) in hot {
            pinned.insert((node_id, fragment_idx));
        }
    }
    
    /// Check if a fragment is pinned for a session
    pub fn is_pinned(&self, session_id: &str, node_id: NodeId, fragment_idx: usize) -> bool {
        if let Some(pinned) = self.pinned_fragments.get(session_id) {
            pinned.contains(&(node_id, fragment_idx))
        } else {
            false
        }
    }
    
    /// Get all pinned fragments for a session
    pub fn get_pinned_fragments(&self, session_id: &str) -> Vec<(NodeId, usize)> {
        if let Some(pinned) = self.pinned_fragments.get(session_id) {
            pinned.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }
    
    /// Update last access time for a session
    fn update_access_time(&mut self, session_id: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_access.insert(session_id.to_string(), now);
    }
    
    /// Evict oldest session if we're at capacity
    fn evict_if_needed(&mut self) {
        if self.plan_caches.len() >= self.max_sessions {
            // Find oldest session
            if let Some(oldest_session) = self.last_access
                .iter()
                .min_by_key(|(_, &time)| time)
                .map(|(session_id, _)| session_id.clone())
            {
                self.plan_caches.remove(&oldest_session);
                self.result_caches.remove(&oldest_session);
                self.hot_fragments.remove(&oldest_session);
                self.last_access.remove(&oldest_session);
            }
        }
    }
    
    /// Clean up expired sessions
    pub fn cleanup_expired(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let expired: Vec<String> = self.last_access
            .iter()
            .filter(|(_, &time)| now - time > self.session_ttl_seconds)
            .map(|(session_id, _)| session_id.clone())
            .collect();
        
        for session_id in expired {
            self.plan_caches.remove(&session_id);
            self.result_caches.remove(&session_id);
            self.hot_fragments.remove(&session_id);
            self.pinned_fragments.remove(&session_id);
            self.last_access.remove(&session_id);
        }
    }
    
    /// Get statistics about sessions
    pub fn stats(&self) -> SessionStats {
        SessionStats {
            active_sessions: self.plan_caches.len(),
            total_cached_plans: self.plan_caches.values().map(|c| c.len()).sum(),
            total_cached_results: self.result_caches.values().map(|c| c.len()).sum(),
        }
    }
}

#[derive(Debug)]
pub struct SessionStats {
    pub active_sessions: usize,
    pub total_cached_plans: usize,
    pub total_cached_results: usize,
}

impl Default for SessionWorkingSet {
    fn default() -> Self {
        Self::new()
    }
}

