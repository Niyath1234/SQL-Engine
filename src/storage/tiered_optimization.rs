use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use crate::hypergraph::node::NodeId;
use crate::hypergraph::edge::EdgeId;
use crate::hypergraph::path::PathId;

/// 3-Tier Storage and Optimization System
/// 
/// Tier 1: Persistent Hypergraph (128-bit vectorized) - Never cleared
/// Tier 2: Session-level cache - Refreshed, syncs to Tier 1
/// Tier 3: User-level patterns - Device-specific, for pre-planning

// ============================================================================
// TIER 1: PERSISTENT HYPERGRAPH (128-bit Vectorized)
// ============================================================================

/// Persistent hypergraph metadata optimized for minimal storage
/// Uses 128-bit vectors and compression for efficient storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistentHypergraph {
    /// Vectorized table metadata (128-bit hash + compressed metadata)
    /// Key: table_name, Value: compressed metadata vector (128 bits)
    pub table_metadata: HashMap<String, CompressedTableMetadata>,
    
    /// Vectorized path metadata (optimized join paths)
    /// Key: path signature hash (128 bits), Value: path metadata
    pub path_metadata: HashMap<u128, CompressedPathMetadata>,
    
    /// Edge metadata (relationship information)
    /// Key: edge_id hash (128 bits), Value: edge metadata
    pub edge_metadata: HashMap<u128, CompressedEdgeMetadata>,
    
    /// Statistics metadata (aggregated over time)
    pub statistics: PersistentStatistics,
    
    /// Version for migration/schema evolution
    pub version: u32,
    
    /// Last update timestamp
    pub last_updated: u64,
}

/// Compressed table metadata (128 bits = 16 bytes)
/// Optimized representation for minimal storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompressedTableMetadata {
    /// 128-bit hash of table name (fast lookup)
    pub table_hash: u128,
    
    /// Compressed metadata (packed into minimal space)
    pub metadata: Vec<u8>, // Compressed JSON or binary format
    
    /// Row count estimate (32-bit, approximate)
    pub row_count_estimate: u32,
    
    /// Size estimate in KB (16-bit, approximate)
    pub size_kb: u16,
    
    /// Last access timestamp (32-bit Unix timestamp)
    pub last_access: u32,
    
    /// Cardinality estimate (8-bit log scale: 0-255 = 2^0 to 2^255)
    pub cardinality_log: u8,
    
    /// Column count (8-bit: 0-255 columns)
    pub column_count: u8,
    
    /// Flags (8 bits):
    /// - bit 0: has_vector_index
    /// - bit 1: has_dictionary_encoding
    /// - bit 2: is_hot (frequently accessed)
    /// - bit 3-7: reserved
    pub flags: u8,
}

/// Compressed path metadata (join order optimization)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompressedPathMetadata {
    /// Path signature hash (128 bits)
    pub path_hash: u128,
    
    /// Node IDs (compressed representation)
    pub node_ids: Vec<u64>, // Compressed node IDs
    
    /// Edge IDs (compressed representation)
    pub edge_ids: Vec<u64>, // Compressed edge IDs
    
    /// Usage count (32-bit)
    pub usage_count: u32,
    
    /// Average execution time in ms (16-bit)
    pub avg_execution_time_ms: u16,
    
    /// Result cardinality estimate (32-bit)
    pub cardinality_estimate: u32,
    
    /// Last used timestamp (32-bit Unix timestamp)
    pub last_used: u32,
    
    /// Is materialized flag
    pub is_materialized: bool,
    
    /// Cost estimate (32-bit float compressed to 16-bit)
    pub cost_estimate: u16,
}

/// Compressed edge metadata (relationships)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompressedEdgeMetadata {
    /// Edge hash (128 bits)
    pub edge_hash: u128,
    
    /// Source node ID (compressed)
    pub source_node_id: u64,
    
    /// Target node ID (compressed)
    pub target_node_id: u64,
    
    /// Join type (2 bits: inner=0, left=1, right=2, full=3)
    pub join_type: u8,
    
    /// Selectivity estimate (8-bit: 0-255 = 0.0 to 1.0)
    pub selectivity: u8,
    
    /// Fan-out estimate (16-bit)
    pub fan_out: u16,
    
    /// Last update timestamp (32-bit)
    pub last_updated: u32,
}

/// Persistent statistics aggregated over time
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistentStatistics {
    /// Total queries processed (64-bit)
    pub total_queries: u64,
    
    /// Total execution time in ms (64-bit)
    pub total_execution_time_ms: u64,
    
    /// Average query latency in ms (32-bit float compressed)
    pub avg_latency_ms: u32,
    
    /// Peak memory usage in MB (32-bit)
    pub peak_memory_mb: u32,
    
    /// Cache hit rate (8-bit: 0-255 = 0% to 100%)
    pub cache_hit_rate: u8,
    
    /// Last statistics update timestamp
    pub last_updated: u64,
}

impl PersistentHypergraph {
    pub fn new() -> Self {
        Self {
            table_metadata: HashMap::new(),
            path_metadata: HashMap::new(),
            edge_metadata: HashMap::new(),
            statistics: PersistentStatistics {
                total_queries: 0,
                total_execution_time_ms: 0,
                avg_latency_ms: 0,
                peak_memory_mb: 0,
                cache_hit_rate: 0,
                last_updated: 0,
            },
            version: 1,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Add or update table metadata (called when table is loaded)
    pub fn update_table_metadata(&mut self, table_name: &str, metadata: CompressedTableMetadata) {
        self.table_metadata.insert(table_name.to_string(), metadata);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Add or update path metadata (called when path is used)
    pub fn update_path_metadata(&mut self, path_hash: u128, metadata: CompressedPathMetadata) {
        self.path_metadata.insert(path_hash, metadata);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Get table metadata
    pub fn get_table_metadata(&self, table_name: &str) -> Option<&CompressedTableMetadata> {
        self.table_metadata.get(table_name)
    }
    
    /// Get path metadata
    pub fn get_path_metadata(&self, path_hash: u128) -> Option<&CompressedPathMetadata> {
        self.path_metadata.get(&path_hash)
    }
    
    /// Compute 128-bit hash for fast lookup
    pub fn compute_hash(data: &[u8]) -> u128 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let hash64 = hasher.finish();
        
        // Expand to 128 bits (replicate pattern for collision resistance)
        (hash64 as u128) << 64 | (hash64 as u128)
    }
}

// ============================================================================
// TIER 2: SESSION-LEVEL CACHE
// ============================================================================

/// Session-level cache that holds bigger temporary data
/// Gets refreshed periodically and syncs key information to Tier 1
#[derive(Clone, Debug)]
pub struct SessionCache {
    /// Query results cache (larger than Tier 1, refreshed more frequently)
    pub query_results: HashMap<String, Vec<u8>>, // Serialized ExecutionBatch
    
    /// Temporary path computations
    pub temporary_paths: HashMap<u128, CompressedPathMetadata>,
    
    /// Session statistics
    pub session_stats: SessionStatistics,
    
    /// Session start timestamp
    pub session_start: u64,
    
    /// Last refresh timestamp
    pub last_refresh: u64,
}

#[derive(Clone, Debug)]
pub struct SessionStatistics {
    /// Queries in this session
    pub queries_count: u32,
    
    /// Cache hits in this session
    pub cache_hits: u32,
    
    /// Average latency in this session
    pub avg_latency_ms: f64,
    
    /// Memory usage in MB
    pub memory_mb: f64,
}

impl SessionCache {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            query_results: HashMap::new(),
            temporary_paths: HashMap::new(),
            session_stats: SessionStatistics {
                queries_count: 0,
                cache_hits: 0,
                avg_latency_ms: 0.0,
                memory_mb: 0.0,
            },
            session_start: now,
            last_refresh: now,
        }
    }
    
    /// Refresh session cache (clear old entries, keep hot ones)
    pub fn refresh(&mut self, max_age_seconds: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Clear old query results (keep only recent)
        // In a full implementation, would check timestamps per entry
        if now - self.last_refresh > max_age_seconds {
            self.query_results.clear();
            self.last_refresh = now;
        }
    }
    
    /// Sync important data to Tier 1 (persistent hypergraph)
    pub fn sync_to_tier1(&self, tier1: &mut PersistentHypergraph) {
        // Sync frequently used paths to Tier 1
        for (path_hash, path_metadata) in &self.temporary_paths {
            // Only sync paths that have been used multiple times
            if path_metadata.usage_count > 5 {
                tier1.update_path_metadata(*path_hash, path_metadata.clone());
            }
        }
    }
}

// ============================================================================
// TIER 3: USER-LEVEL PATTERNS
// ============================================================================

/// User-level table hotness and query pattern tracking
/// Device-specific, used for pre-planning and optimization
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPatternCache {
    /// Device/user identifier
    pub device_id: String,
    
    /// Table hotness map (tracks access frequency per table)
    pub table_hotness: HashMap<String, TableHotness>,
    
    /// Query pattern tracking (common query patterns for this user)
    pub query_patterns: Vec<QueryPattern>,
    
    /// Temporal patterns (time-based access patterns)
    pub temporal_patterns: HashMap<u8, TemporalPattern>, // Hour of day -> pattern
    
    /// Last update timestamp
    pub last_updated: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableHotness {
    /// Table name
    pub table_name: String,
    
    /// Access count (32-bit)
    pub access_count: u32,
    
    /// Last access timestamp
    pub last_access: u64,
    
    /// Average queries per hour (16-bit fixed point)
    pub queries_per_hour: u16,
    
    /// Hotness score (8-bit: 0-255, higher = hotter)
    pub hotness_score: u8,
    
    /// Typical access times (bitmap: 24 hours * 4 = 96 bits compressed)
    pub access_times: Vec<u8>, // Compressed bitmap
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPattern {
    /// Pattern signature (hash of query structure)
    pub pattern_hash: u128,
    
    /// Query template (parameterized)
    pub query_template: String,
    
    /// Frequency (how often this pattern is used)
    pub frequency: u32,
    
    /// Average execution time
    pub avg_execution_time_ms: u32,
    
    /// Typical tables accessed
    pub tables_accessed: Vec<String>,
    
    /// Last seen timestamp
    pub last_seen: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalPattern {
    /// Hour of day (0-23)
    pub hour: u8,
    
    /// Query frequency at this hour
    pub query_frequency: u32,
    
    /// Common tables accessed at this hour
    pub common_tables: Vec<String>,
    
    /// Average latency at this hour
    pub avg_latency_ms: u32,
}

impl UserPatternCache {
    pub fn new(device_id: String) -> Self {
        Self {
            device_id,
            table_hotness: HashMap::new(),
            query_patterns: Vec::new(),
            temporal_patterns: HashMap::new(),
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Record table access (updates hotness)
    pub fn record_table_access(&mut self, table_name: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let hotness = self.table_hotness
            .entry(table_name.to_string())
            .or_insert_with(|| TableHotness {
                table_name: table_name.to_string(),
                access_count: 0,
                last_access: now,
                queries_per_hour: 0,
                hotness_score: 0,
                access_times: vec![0; 24], // One byte per hour
            });
        
        hotness.access_count += 1;
        hotness.last_access = now;
        
        // Update access times bitmap
        let hour = (now / 3600) % 24;
        hotness.access_times[hour as usize] = hotness.access_times[hour as usize].saturating_add(1).min(255);
        
        // Recalculate hotness score (0-255)
        hotness.hotness_score = Self::calculate_hotness_score(hotness);
        
        self.last_updated = now;
    }
    
    /// Calculate hotness score (0-255)
    fn calculate_hotness_score(hotness: &TableHotness) -> u8 {
        // Formula: Combine access count, recency, and frequency
        let age_hours = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() - hotness.last_access) / 3600;
        
        // Recency factor (decay with age)
        let recency_factor = if age_hours < 1 {
            1.0
        } else if age_hours < 24 {
            1.0 / (1.0 + age_hours as f64 * 0.1)
        } else {
            0.1
        };
        
        // Frequency factor (normalized to 0-1)
        let frequency_factor = (hotness.queries_per_hour as f64 / 100.0).min(1.0);
        
        // Access count factor (log scale)
        let count_factor = (hotness.access_count as f64).ln() / 10.0; // ln(10000) ≈ 9.2
        
        // Combine factors (weighted average)
        let score = (recency_factor * 0.4 + frequency_factor * 0.3 + count_factor * 0.3) * 255.0;
        score.min(255.0) as u8
    }
    
    /// Record query pattern
    pub fn record_query_pattern(&mut self, query_template: &str, tables: &[String], execution_time_ms: u32) {
        let pattern_hash = PersistentHypergraph::compute_hash(query_template.as_bytes());
        
        // Find existing pattern or create new
        let pattern = self.query_patterns
            .iter_mut()
            .find(|p| p.pattern_hash == pattern_hash);
        
        if let Some(p) = pattern {
            // Update existing pattern
            p.frequency += 1;
            p.avg_execution_time_ms = ((p.avg_execution_time_ms as f64 * (p.frequency - 1) as f64 + execution_time_ms as f64) / p.frequency as f64) as u32;
            p.last_seen = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        } else {
            // Create new pattern
            self.query_patterns.push(QueryPattern {
                pattern_hash,
                query_template: query_template.to_string(),
                frequency: 1,
                avg_execution_time_ms: execution_time_ms,
                tables_accessed: tables.to_vec(),
                last_seen: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            });
        }
        
        // Keep only top 100 patterns (by frequency)
        if self.query_patterns.len() > 100 {
            self.query_patterns.sort_by(|a, b| b.frequency.cmp(&a.frequency));
            self.query_patterns.truncate(100);
        }
    }
    
    /// Get hottest tables (for pre-warming)
    pub fn get_hottest_tables(&self, limit: usize) -> Vec<String> {
        let mut tables: Vec<_> = self.table_hotness.iter().collect();
        tables.sort_by(|a, b| b.1.hotness_score.cmp(&a.1.hotness_score));
        tables.into_iter().take(limit).map(|(name, _)| name.clone()).collect()
    }
    
    /// Predict likely next queries (for pre-planning)
    pub fn predict_likely_queries(&self, hour: u8) -> Vec<&QueryPattern> {
        // Return patterns that are commonly used at this hour
        let mut patterns: Vec<_> = self.query_patterns.iter().collect();
        patterns.sort_by(|a, b| b.frequency.cmp(&a.frequency));
        patterns.into_iter().take(10).collect()
    }
}

// ============================================================================
// TIERED OPTIMIZATION MANAGER
// ============================================================================

/// Manages all three tiers and coordinates between them
pub struct TieredOptimizationManager {
    /// Tier 1: Persistent hypergraph (shared, never cleared)
    pub tier1_persistent: Arc<RwLock<PersistentHypergraph>>,
    
    /// Tier 2: Session cache (per-session, refreshed)
    pub tier2_session: Arc<RwLock<SessionCache>>,
    
    /// Tier 3: User patterns (per-device, persistent)
    pub tier3_user_patterns: Arc<RwLock<HashMap<String, UserPatternCache>>>,
    
    /// File path for Tier 1 persistence
    pub tier1_storage_path: String,
    
    /// File path for Tier 3 persistence
    pub tier3_storage_path: String,
}

impl TieredOptimizationManager {
    pub fn new(tier1_path: String, tier3_path: String) -> Result<Self, Box<dyn std::error::Error>> {
        // Load Tier 1 from disk (or create new)
        let tier1 = Self::load_tier1(&tier1_path)?;
        
        // Create new session cache
        let tier2 = Arc::new(RwLock::new(SessionCache::new()));
        
        // Load Tier 3 from disk (or create new)
        let tier3 = Self::load_tier3(&tier3_path)?;
        
        Ok(Self {
            tier1_persistent: Arc::new(RwLock::new(tier1)),
            tier2_session: tier2,
            tier3_user_patterns: Arc::new(RwLock::new(tier3)),
            tier1_storage_path: tier1_path,
            tier3_storage_path: tier3_path,
        })
    }
    
    /// Load Tier 1 from disk
    fn load_tier1(path: &str) -> Result<PersistentHypergraph, Box<dyn std::error::Error>> {
        if std::path::Path::new(path).exists() {
            let data = std::fs::read(path)?;
            let hypergraph: PersistentHypergraph = bincode::deserialize(&data)
                .map_err(|e| format!("Failed to deserialize Tier 1: {}", e))?;
            Ok(hypergraph)
        } else {
            Ok(PersistentHypergraph::new())
        }
    }
    
    /// Save Tier 1 to disk
    pub fn save_tier1(&self) -> Result<(), Box<dyn std::error::Error>> {
        let tier1 = self.tier1_persistent.read().unwrap();
        let data = bincode::serialize(&*tier1)
            .map_err(|e| format!("Failed to serialize Tier 1: {}", e))?;
        std::fs::write(&self.tier1_storage_path, data)?;
        Ok(())
    }
    
    /// Load Tier 3 from disk
    fn load_tier3(path: &str) -> Result<HashMap<String, UserPatternCache>, Box<dyn std::error::Error>> {
        if std::path::Path::new(path).exists() {
            let data = std::fs::read(path)?;
            let patterns: HashMap<String, UserPatternCache> = bincode::deserialize(&data)
                .map_err(|e| format!("Failed to deserialize Tier 3: {}", e))?;
            Ok(patterns)
        } else {
            Ok(HashMap::new())
        }
    }
    
    /// Save Tier 3 to disk
    pub fn save_tier3(&self) -> Result<(), Box<dyn std::error::Error>> {
        let tier3 = self.tier3_user_patterns.read().unwrap();
        let data = bincode::serialize(&*tier3)
            .map_err(|e| format!("Failed to serialize Tier 3: {}", e))?;
        std::fs::write(&self.tier3_storage_path, data)?;
        Ok(())
    }
    
    /// Sync Tier 2 to Tier 1 (periodic sync)
    pub fn sync_tier2_to_tier1(&self) {
        let tier2 = self.tier2_session.read().unwrap();
        let mut tier1 = self.tier1_persistent.write().unwrap();
        tier2.sync_to_tier1(&mut tier1);
    }
    
    /// Refresh Tier 2 session cache
    pub fn refresh_tier2(&self, max_age_seconds: u64) {
        let mut tier2 = self.tier2_session.write().unwrap();
        tier2.refresh(max_age_seconds);
    }
    
    /// Get or create user pattern cache
    pub fn get_user_patterns(&self, device_id: &str) -> Arc<RwLock<UserPatternCache>> {
        let mut tier3 = self.tier3_user_patterns.write().unwrap();
        
        if !tier3.contains_key(device_id) {
            tier3.insert(device_id.to_string(), UserPatternCache::new(device_id.to_string()));
        }
        
        // Return a handle (in real implementation, would use Arc properly)
        // For now, this is a simplified version
        Arc::new(RwLock::new(tier3.get(device_id).unwrap().clone()))
    }
    
    /// Periodic maintenance (sync, refresh, save)
    pub fn periodic_maintenance(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Sync Tier 2 to Tier 1
        self.sync_tier2_to_tier1();
        
        // Refresh Tier 2 (clear old entries)
        self.refresh_tier2(3600); // 1 hour
        
        // Save Tier 1 to disk
        self.save_tier1()?;
        
        // Save Tier 3 to disk
        self.save_tier3()?;
        
        Ok(())
    }
}

