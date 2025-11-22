/// Multi-tier memory model
/// RAM is treated as L1 (fastest), with support for disk/SSD as L2/L3
use crate::hypergraph::node::NodeId;
use std::sync::Arc;
use dashmap::DashMap;

/// Memory tier levels
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemoryTier {
    /// L1: RAM (fastest, most expensive)
    L1,
    /// L2: Compressed RAM (medium speed, lower cost)
    L2,
    /// L3: Disk/SSD (slowest, cheapest)
    L3,
}

impl MemoryTier {
    pub fn access_latency_ns(&self) -> u64 {
        match self {
            MemoryTier::L1 => 100,      // ~100ns for RAM access
            MemoryTier::L2 => 1000,     // ~1µs for compressed RAM (decompression overhead)
            MemoryTier::L3 => 100_000,  // ~100µs for SSD access
        }
    }
    
    pub fn cost_per_mb(&self) -> f64 {
        match self {
            MemoryTier::L1 => 1.0,      // Baseline cost
            MemoryTier::L2 => 0.3,      // Compressed takes less space
            MemoryTier::L3 => 0.01,     // Disk is much cheaper
        }
    }
}

/// Fragment access statistics for tier management
#[derive(Clone, Debug)]
pub struct FragmentAccessStats {
    /// Number of times accessed
    pub access_count: u64,
    
    /// Last access time (nanoseconds since epoch)
    pub last_access_ns: u64,
    
    /// First access time
    pub first_access_ns: u64,
    
    /// Total bytes accessed
    pub total_bytes_accessed: u64,
    
    /// Current memory tier
    pub current_tier: MemoryTier,
    
    /// Access frequency (accesses per second)
    pub access_frequency: f64,
    
    /// Hotness score (higher = hotter, should stay in L1)
    pub hotness_score: f64,
}

impl FragmentAccessStats {
    pub fn new() -> Self {
        let now = now_ns();
        Self {
            access_count: 0,
            last_access_ns: now,
            first_access_ns: now,
            total_bytes_accessed: 0,
            current_tier: MemoryTier::L1,
            access_frequency: 0.0,
            hotness_score: 0.0,
        }
    }
    
    pub fn record_access(&mut self, bytes: usize) {
        let now = now_ns();
        self.access_count += 1;
        self.last_access_ns = now;
        self.total_bytes_accessed += bytes as u64;
        
        // Update access frequency (exponential moving average)
        let time_since_first = (now - self.first_access_ns) as f64 / 1_000_000_000.0;
        if time_since_first > 0.0 {
            self.access_frequency = self.access_count as f64 / time_since_first;
        }
        
        // Update hotness score (weighted by recency and frequency)
        self.update_hotness_score();
    }
    
    fn update_hotness_score(&mut self) {
        let now = now_ns();
        let recency_weight = 1.0 / (1.0 + ((now - self.last_access_ns) as f64 / 1_000_000_000.0)); // Decay over seconds
        let frequency_weight = self.access_frequency / 1000.0; // Normalize frequency
        let volume_weight = (self.total_bytes_accessed as f64 / 1_000_000.0).min(10.0); // Cap at 10
        
        // Hotness = weighted combination of recency, frequency, and volume
        self.hotness_score = (recency_weight * 0.4 + frequency_weight * 0.4 + volume_weight * 0.2) * 100.0;
    }
    
    pub fn should_promote_to_l1(&self) -> bool {
        // Promote if hotness score is high and currently not in L1
        self.hotness_score > 50.0 && self.current_tier != MemoryTier::L1
    }
    
    pub fn should_demote_from_l1(&self) -> bool {
        // Demote if hotness score is low and currently in L1
        self.hotness_score < 10.0 && self.current_tier == MemoryTier::L1
    }
}

/// Multi-tier memory manager
/// Manages fragment placement across memory tiers based on access patterns
pub struct MultiTierMemoryManager {
    /// Fragment access statistics
    access_stats: DashMap<NodeId, FragmentAccessStats>,
    
    /// L1 capacity in bytes (RAM)
    l1_capacity_bytes: usize,
    
    /// L2 capacity in bytes (compressed RAM)
    l2_capacity_bytes: usize,
    
    /// Current L1 usage
    l1_usage_bytes: Arc<std::sync::atomic::AtomicUsize>,
    
    /// Current L2 usage
    l2_usage_bytes: Arc<std::sync::atomic::AtomicUsize>,
    
    /// Fragments currently in L1
    l1_fragments: DashMap<NodeId, usize>, // NodeId -> size in bytes
    
    /// Fragments currently in L2
    l2_fragments: DashMap<NodeId, usize>,
    
    /// Fragments currently in L3 (on disk)
    l3_fragments: DashMap<NodeId, usize>,
    
    /// Prefetch queue (fragments to promote)
    prefetch_queue: Arc<dashmap::DashSet<NodeId>>,
}

impl MultiTierMemoryManager {
    pub fn new(l1_capacity_mb: usize, l2_capacity_mb: usize) -> Self {
        Self {
            access_stats: DashMap::new(),
            l1_capacity_bytes: l1_capacity_mb * 1024 * 1024,
            l2_capacity_bytes: l2_capacity_mb * 1024 * 1024,
            l1_usage_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            l2_usage_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            l1_fragments: DashMap::new(),
            l2_fragments: DashMap::new(),
            l3_fragments: DashMap::new(),
            prefetch_queue: Arc::new(dashmap::DashSet::new()),
        }
    }
    
    /// Record fragment access
    pub fn record_access(&self, node_id: NodeId, fragment_size_bytes: usize) {
        let mut stats = self.access_stats
            .entry(node_id)
            .or_insert_with(|| FragmentAccessStats::new());
        
        stats.record_access(fragment_size_bytes);
        
        // Check if we should prefetch (promote to L1)
        if stats.should_promote_to_l1() {
            self.prefetch_queue.insert(node_id);
        }
    }
    
    /// Get fragment tier
    pub fn get_tier(&self, node_id: NodeId) -> MemoryTier {
        if self.l1_fragments.contains_key(&node_id) {
            MemoryTier::L1
        } else if self.l2_fragments.contains_key(&node_id) {
            MemoryTier::L2
        } else if self.l3_fragments.contains_key(&node_id) {
            MemoryTier::L3
        } else {
            // Default: assume in L1 if not tracked
            MemoryTier::L1
        }
    }
    
    /// Register fragment in L1
    pub fn register_l1(&self, node_id: NodeId, size_bytes: usize) {
        // Remove from other tiers
        self.l2_fragments.remove(&node_id);
        self.l3_fragments.remove(&node_id);
        
        // Add to L1
        self.l1_fragments.insert(node_id, size_bytes);
        self.l1_usage_bytes.fetch_add(size_bytes, std::sync::atomic::Ordering::Relaxed);
        
        // Update stats
        if let Some(mut stats) = self.access_stats.get_mut(&node_id) {
            stats.current_tier = MemoryTier::L1;
        }
    }
    
    /// Register fragment in L2
    pub fn register_l2(&self, node_id: NodeId, size_bytes: usize) {
        // Remove from other tiers
        self.l1_fragments.remove(&node_id);
        self.l3_fragments.remove(&node_id);
        
        // Subtract from L1 usage if it was there
        if let Some((_, old_size)) = self.l1_fragments.remove(&node_id) {
            self.l1_usage_bytes.fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }
        
        // Add to L2
        self.l2_fragments.insert(node_id, size_bytes);
        self.l2_usage_bytes.fetch_add(size_bytes, std::sync::atomic::Ordering::Relaxed);
        
        // Update stats
        if let Some(mut stats) = self.access_stats.get_mut(&node_id) {
            stats.current_tier = MemoryTier::L2;
        }
    }
    
    /// Register fragment in L3
    pub fn register_l3(&self, node_id: NodeId, size_bytes: usize) {
        // Remove from other tiers
        self.l1_fragments.remove(&node_id);
        self.l2_fragments.remove(&node_id);
        
        // Subtract from L1/L2 usage if it was there
        if let Some((_, old_size)) = self.l1_fragments.remove(&node_id) {
            self.l1_usage_bytes.fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some((_, old_size)) = self.l2_fragments.remove(&node_id) {
            self.l2_usage_bytes.fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }
        
        // Add to L3
        self.l3_fragments.insert(node_id, size_bytes);
        
        // Update stats
        if let Some(mut stats) = self.access_stats.get_mut(&node_id) {
            stats.current_tier = MemoryTier::L3;
        }
    }
    
    /// Evict cold fragments from L1 to make room
    pub fn evict_cold_fragments(&self, target_bytes: usize) -> Vec<NodeId> {
        let mut evicted = Vec::new();
        let mut candidates: Vec<(NodeId, f64, usize)> = Vec::new();
        
        // Collect candidates with their hotness scores
        for entry in self.l1_fragments.iter() {
            let node_id = *entry.key();
            let size = *entry.value();
            let hotness = self.access_stats
                .get(&node_id)
                .map(|s| s.hotness_score)
                .unwrap_or(0.0);
            
            candidates.push((node_id, hotness, size));
        }
        
        // Sort by hotness (lowest first = coldest)
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Evict coldest fragments until we free enough space
        let mut freed = 0;
        for (node_id, _hotness, size) in candidates {
            if freed >= target_bytes {
                break;
            }
            
            // Demote to L2 (compressed)
            self.register_l2(node_id, size);
            evicted.push(node_id);
            freed += size;
        }
        
        evicted
    }
    
    /// Promote hot fragments from L2/L3 to L1
    pub fn promote_hot_fragments(&self, available_bytes: usize) -> Vec<NodeId> {
        let mut promoted = Vec::new();
        let mut candidates: Vec<(NodeId, f64, usize)> = Vec::new();
        
        // Collect candidates from L2 and L3
        for entry in self.l2_fragments.iter() {
            let node_id = *entry.key();
            let size = *entry.value();
            let hotness = self.access_stats
                .get(&node_id)
                .map(|s| s.hotness_score)
                .unwrap_or(0.0);
            
            candidates.push((node_id, hotness, size));
        }
        
        for entry in self.l3_fragments.iter() {
            let node_id = *entry.key();
            let size = *entry.value();
            let hotness = self.access_stats
                .get(&node_id)
                .map(|s| s.hotness_score)
                .unwrap_or(0.0);
            
            candidates.push((node_id, hotness, size));
        }
        
        // Sort by hotness (highest first = hottest)
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Promote hottest fragments until we run out of space
        let mut used = 0;
        for (node_id, _hotness, size) in candidates {
            if used + size > available_bytes {
                break;
            }
            
            // Check if we need to evict to make room
            let current_l1_usage = self.l1_usage_bytes.load(std::sync::atomic::Ordering::Relaxed);
            if current_l1_usage + size > self.l1_capacity_bytes {
                // Evict cold fragments
                let needed = (current_l1_usage + size) - self.l1_capacity_bytes;
                self.evict_cold_fragments(needed);
            }
            
            // Promote to L1
            self.register_l1(node_id, size);
            promoted.push(node_id);
            used += size;
        }
        
        promoted
    }
    
    /// Process prefetch queue (background task)
    pub fn process_prefetch_queue(&self) {
        let mut to_promote: Vec<NodeId> = self.prefetch_queue.iter().map(|n| *n).collect();
        self.prefetch_queue.clear();
        
        if to_promote.is_empty() {
            return;
        }
        
        // Calculate available space
        let current_l1_usage = self.l1_usage_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let available = self.l1_capacity_bytes.saturating_sub(current_l1_usage);
        
        // Promote fragments
        for node_id in to_promote {
            if let Some(size) = self.l2_fragments.get(&node_id).or_else(|| self.l3_fragments.get(&node_id)) {
                let size_bytes = *size.value();
                if available >= size_bytes {
                    self.register_l1(node_id, size_bytes);
                }
            }
        }
    }
    
    /// Get memory tier statistics
    pub fn get_tier_stats(&self) -> TierStatistics {
        TierStatistics {
            l1_usage_bytes: self.l1_usage_bytes.load(std::sync::atomic::Ordering::Relaxed),
            l1_capacity_bytes: self.l1_capacity_bytes,
            l2_usage_bytes: self.l2_usage_bytes.load(std::sync::atomic::Ordering::Relaxed),
            l2_capacity_bytes: self.l2_capacity_bytes,
            l1_fragment_count: self.l1_fragments.len(),
            l2_fragment_count: self.l2_fragments.len(),
            l3_fragment_count: self.l3_fragments.len(),
        }
    }
    
    /// Get hot fragments (for monitoring/debugging)
    pub fn get_hot_fragments(&self, top_n: usize) -> Vec<(NodeId, FragmentAccessStats)> {
        let mut fragments: Vec<(NodeId, FragmentAccessStats)> = self.access_stats
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        
        fragments.sort_by(|a, b| b.1.hotness_score.partial_cmp(&a.1.hotness_score).unwrap_or(std::cmp::Ordering::Equal));
        fragments.truncate(top_n);
        fragments
    }
    
    /// Clean up cold/unused fragments periodically
    /// Evicts fragments that haven't been accessed in a long time
    pub fn cleanup_cold_fragments(&self, max_age_seconds: u64, max_fragments_to_evict: usize) -> usize {
        let now = now_ns();
        let max_age_ns = max_age_seconds as u64 * 1_000_000_000;
        let mut evicted_count = 0;
        
        // Collect cold fragments that haven't been accessed recently
        let mut cold_fragments: Vec<(NodeId, usize)> = Vec::new();
        
        for entry in self.access_stats.iter() {
            let node_id = *entry.key();
            let stats = entry.value();
            
            // Check if fragment is cold (not accessed in max_age_seconds)
            let age_ns = now.saturating_sub(stats.last_access_ns);
            if age_ns > max_age_ns && stats.hotness_score < 5.0 {
                // Get fragment size
                let size = self.l1_fragments.get(&node_id)
                    .or_else(|| self.l2_fragments.get(&node_id))
                    .or_else(|| self.l3_fragments.get(&node_id))
                    .map(|e| *e.value())
                    .unwrap_or(0);
                
                if size > 0 {
                    cold_fragments.push((node_id, size));
                }
            }
        }
        
        // Sort by last access time (oldest first) and hotness (lowest first)
        cold_fragments.sort_by(|a, b| {
            let stats_a = self.access_stats.get(&a.0);
            let stats_b = self.access_stats.get(&b.0);
            
            match (stats_a, stats_b) {
                (Some(stat_a), Some(stat_b)) => {
                    // Sort by last access time first (oldest first)
                    stat_a.last_access_ns.cmp(&stat_b.last_access_ns)
                        .then_with(|| stat_a.hotness_score.partial_cmp(&stat_b.hotness_score).unwrap_or(std::cmp::Ordering::Equal))
                }
                _ => std::cmp::Ordering::Equal,
            }
        });
        
        // Evict cold fragments (up to max_fragments_to_evict)
        for (node_id, _size) in cold_fragments.iter().take(max_fragments_to_evict) {
            // Evict from L1 if present, otherwise from L2
            if self.l1_fragments.contains_key(node_id) {
                if let Some((_, size_bytes)) = self.l1_fragments.remove(node_id) {
                    self.l1_usage_bytes.fetch_sub(size_bytes, std::sync::atomic::Ordering::Relaxed);
                    // Demote to L2 (compressed)
                    self.register_l2(*node_id, size_bytes);
                    evicted_count += 1;
                }
            } else if self.l2_fragments.contains_key(node_id) {
                // For very old fragments in L2, we could move to L3, but for now just mark as less hot
                // The access stats will decay naturally
                evicted_count += 1;
            }
        }
        
        evicted_count
    }
}

/// Memory tier statistics
#[derive(Debug, Clone)]
pub struct TierStatistics {
    pub l1_usage_bytes: usize,
    pub l1_capacity_bytes: usize,
    pub l2_usage_bytes: usize,
    pub l2_capacity_bytes: usize,
    pub l1_fragment_count: usize,
    pub l2_fragment_count: usize,
    pub l3_fragment_count: usize,
}

impl TierStatistics {
    pub fn l1_usage_percent(&self) -> f64 {
        if self.l1_capacity_bytes > 0 {
            (self.l1_usage_bytes as f64 / self.l1_capacity_bytes as f64) * 100.0
        } else {
            0.0
        }
    }
    
    pub fn l2_usage_percent(&self) -> f64 {
        if self.l2_capacity_bytes > 0 {
            (self.l2_usage_bytes as f64 / self.l2_capacity_bytes as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Helper function to get current time in nanoseconds
fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

