//! Level 3: User Pattern Map
//!
//! Client-side/user device storage that:
//! - Tracks table hotness (access frequency)
//! - Analyzes query patterns
//! - Enables pre-planning based on user behavior
//! - Stored on user device (localStorage, IndexedDB, etc.)

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::PathBuf;

/// Table hotness tracking
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableHotness {
    /// Table name
    pub table_name: String,
    
    /// Access count (weighted by recency)
    pub access_count: u32,
    
    /// Last accessed timestamp
    pub last_accessed: u64,
    
    /// Access frequency (accesses per day, exponentially weighted)
    pub frequency: f64,
    
    /// Time of day pattern (hour -> count, for detecting patterns)
    pub hourly_pattern: HashMap<u8, u32>,
}

/// Query pattern (for pre-planning)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPattern {
    /// Query signature hash
    pub signature_hash: u64,
    
    /// Query template (with parameters removed)
    pub template: String,
    
    /// Execution count
    pub execution_count: u32,
    
    /// Average execution time (ms)
    pub avg_execution_time_ms: f64,
    
    /// Tables accessed
    pub tables: Vec<String>,
    
    /// Last executed timestamp
    pub last_executed: u64,
    
    /// Execution time pattern (for pre-planning)
    pub execution_times: Vec<f64>,
}

/// User pattern map (Level 3)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPatternMap {
    /// User ID
    pub user_id: String,
    
    /// Table hotness tracking
    pub table_hotness: HashMap<String, TableHotness>,
    
    /// Query patterns
    pub query_patterns: HashMap<u64, QueryPattern>,
    
    /// Preferred join orders (learned from user behavior)
    pub preferred_joins: HashMap<String, Vec<String>>, // table -> preferred join order
    
    /// Last updated timestamp
    pub last_updated: u64,
}

impl UserPatternMap {
    /// Load from user device storage
    pub fn load_from_device(user_id: &str) -> Result<Self, anyhow::Error> {
        // Try to load from localStorage (browser) or file (desktop)
        if let Ok(data) = Self::load_from_local_storage(user_id) {
            Ok(data)
        } else if let Ok(data) = Self::load_from_file(user_id) {
            Ok(data)
        } else {
            // Create new pattern map
            Ok(Self::new(user_id))
        }
    }
    
    /// Save to user device storage
    pub fn save_to_device(&mut self) -> Result<(), anyhow::Error> {
        // Update timestamp before saving
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Try localStorage first, fallback to file
        if let Err(_) = self.save_to_local_storage() {
            self.save_to_file()?;
        }
        Ok(())
    }
    
    /// Record table access
    pub fn record_table_access(&mut self, table_name: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let hotness = self.table_hotness.entry(table_name.to_string())
            .or_insert_with(|| TableHotness {
                table_name: table_name.to_string(),
                access_count: 0,
                last_accessed: now,
                frequency: 0.0,
                hourly_pattern: HashMap::new(),
            });
        
        hotness.access_count += 1;
        hotness.last_accessed = now;
        
        // Update frequency with exponential decay
        let hours_since_last = if hotness.last_accessed > 0 {
            (now.saturating_sub(hotness.last_accessed)) as f64 / 3600.0
        } else {
            1.0
        };
        hotness.frequency = hotness.frequency * 0.9 + (1.0 / hours_since_last.max(0.1)) * 0.1;
        
        // Record hourly pattern
        let hour = (now / 3600) % 24;
        *hotness.hourly_pattern.entry(hour as u8).or_insert(0) += 1;
    }
    
    /// Record query execution
    pub fn record_query(&mut self, query_template: &str, tables: Vec<String>, execution_time_ms: f64) {
        let signature_hash = Self::hash_query_template(query_template);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let pattern = self.query_patterns.entry(signature_hash)
            .or_insert_with(|| QueryPattern {
                signature_hash,
                template: query_template.to_string(),
                execution_count: 0,
                avg_execution_time_ms: 0.0,
                tables: tables.clone(),
                last_executed: now,
                execution_times: Vec::new(),
            });
        
        pattern.execution_count += 1;
        pattern.last_executed = now;
        
        // Update average execution time
        pattern.execution_times.push(execution_time_ms);
        if pattern.execution_times.len() > 100 {
            pattern.execution_times.remove(0); // Keep last 100
        }
        pattern.avg_execution_time_ms = pattern.execution_times.iter().sum::<f64>() / pattern.execution_times.len() as f64;
        
        // Record table accesses
        for table in tables {
            self.record_table_access(&table);
        }
    }
    
    /// Get hot tables (sorted by frequency)
    pub fn get_hot_tables(&self, limit: usize) -> Vec<&TableHotness> {
        let mut hot: Vec<&TableHotness> = self.table_hotness.values().collect();
        hot.sort_by(|a, b| b.frequency.partial_cmp(&a.frequency).unwrap());
        hot.truncate(limit);
        hot
    }
    
    /// Get predicted next queries (for pre-planning)
    pub fn get_predicted_queries(&self, limit: usize) -> Vec<&QueryPattern> {
        let mut patterns: Vec<&QueryPattern> = self.query_patterns.values().collect();
        
        // Sort by recency and frequency
        patterns.sort_by(|a, b| {
            // Prioritize recent, frequent queries
            let score_a = (a.execution_count as f64) * (1.0 / (SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_secs().saturating_sub(a.last_executed) as f64 + 1.0));
            let score_b = (b.execution_count as f64) * (1.0 / (SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_secs().saturating_sub(b.last_executed) as f64 + 1.0));
            score_b.partial_cmp(&score_a).unwrap()
        });
        
        patterns.truncate(limit);
        patterns
    }
    
    /// Create new pattern map
    pub fn new(user_id: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
            table_hotness: HashMap::new(),
            query_patterns: HashMap::new(),
            preferred_joins: HashMap::new(),
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Load from localStorage (browser environment)
    fn load_from_local_storage(user_id: &str) -> Result<Self, anyhow::Error> {
        // In browser, this would use web_sys or wasm-bindgen
        // For now, return error (desktop implementation)
        anyhow::bail!("localStorage not available (not in browser)")
    }
    
    /// Save to localStorage (browser environment)
    fn save_to_local_storage(&self) -> Result<(), anyhow::Error> {
        // In browser, this would use web_sys or wasm-bindgen
        anyhow::bail!("localStorage not available (not in browser)")
    }
    
    /// Load from file (desktop environment)
    fn load_from_file(user_id: &str) -> Result<Self, anyhow::Error> {
        use std::fs;
        use std::path::PathBuf;
        
        let path = Self::get_file_path(user_id)?;
        if path.exists() {
            let data = fs::read_to_string(path)?;
            let pattern_map: UserPatternMap = serde_json::from_str(&data)?;
            Ok(pattern_map)
        } else {
            anyhow::bail!("Pattern file not found")
        }
    }
    
    /// Save to file (desktop environment)
    fn save_to_file(&mut self) -> Result<(), anyhow::Error> {
        use std::fs;
        use std::path::PathBuf;
        
        let path = Self::get_file_path(&self.user_id)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let json = serde_json::to_string_pretty(self)?;
        fs::write(path, json)?;
        Ok(())
    }
    
    /// Get file path for user patterns
    fn get_file_path(user_id: &str) -> Result<PathBuf, anyhow::Error> {
        let mut path = dirs::data_local_dir()
            .ok_or_else(|| anyhow::anyhow!("Cannot find data directory"))?;
        path.push("hypergraph-sql-engine");
        path.push("user-patterns");
        path.push(format!("{}.json", user_id));
        Ok(path)
    }
    
    /// Hash query template to 64 bits
    fn hash_query_template(template: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        template.hash(&mut hasher);
        hasher.finish()
    }
}


