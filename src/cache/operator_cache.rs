/// Persistent compiled operator cache
/// Persists compiled operators (WASM modules, micro-kernel signatures) to disk
/// and reuses across restarts and across sessions
use std::path::PathBuf;
use std::collections::{HashMap, VecDeque};
use std::fs;
use anyhow::Result;

/// Cache entry for a compiled operator
pub struct CacheEntry {
    pub key: String,
    pub path: PathBuf,
    pub size: u64,
}

/// Operator cache with LRU eviction policy
pub struct OperatorCache {
    pub dir: PathBuf,
    pub index: HashMap<String, CacheEntry>,
    pub lru: VecDeque<String>,
    pub max_bytes: u64,
    pub used_bytes: u64,
}

impl OperatorCache {
    /// Create a new operator cache
    pub fn new(dir: PathBuf, max_bytes: u64) -> Self {
        // Ensure cache directory exists
        fs::create_dir_all(&dir).ok();
        
        Self {
            dir,
            index: HashMap::new(),
            lru: VecDeque::new(),
            max_bytes,
            used_bytes: 0,
        }
    }
    
    /// Put a compiled operator into the cache
    /// Returns the path to the cached file
    pub fn put(&mut self, key: &str, bytes: &[u8]) -> Result<PathBuf> {
        // Generate cache file path
        let path = self.dir.join(format!("op_{}.bin", key));
        
        // Write bytes to disk
        std::fs::write(&path, bytes)?;
        
        let size = bytes.len() as u64;
        
        // Evict entries if needed to make space
        while self.used_bytes + size > self.max_bytes {
            self.evict_one()?;
        }
        
        // Add to index and LRU
        let entry = CacheEntry {
            key: key.to_string(),
            path: path.clone(),
            size,
        };
        
        // Remove old entry if it exists
        if let Some(old_entry) = self.index.remove(key) {
            self.used_bytes = self.used_bytes.saturating_sub(old_entry.size);
            self.lru.retain(|k| k != key);
        }
        
        self.index.insert(key.to_string(), entry);
        self.lru.push_back(key.to_string());
        self.used_bytes += size;
        
        Ok(path)
    }
    
    /// Get a cached operator by key
    /// Returns the path to the cached file if found, None otherwise
    pub fn get(&mut self, key: &str) -> Option<PathBuf> {
        if let Some(entry) = self.index.get(key) {
            // Verify file still exists
            if entry.path.exists() {
                // Bump to end of LRU (most recently used)
                self.lru.retain(|k| k != key);
                self.lru.push_back(key.to_string());
                return Some(entry.path.clone());
            } else {
                // File was deleted externally, remove from cache
                let entry_size = entry.size;
                self.index.remove(key);
                self.used_bytes = self.used_bytes.saturating_sub(entry_size);
            }
        }
        None
    }
    
    /// Evict the least recently used entry
    fn evict_one(&mut self) -> Result<()> {
        if let Some(old_key) = self.lru.pop_front() {
            if let Some(entry) = self.index.remove(&old_key) {
                // Remove file from disk
                let _ = fs::remove_file(&entry.path);
                self.used_bytes = self.used_bytes.saturating_sub(entry.size);
            }
        }
        Ok(())
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> (usize, u64, u64) {
        (self.index.len(), self.used_bytes, self.max_bytes)
    }
    
    /// Clear all cache entries
    pub fn clear(&mut self) -> Result<()> {
        for entry in self.index.values() {
            let _ = fs::remove_file(&entry.path);
        }
        self.index.clear();
        self.lru.clear();
        self.used_bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_operator_cache_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = OperatorCache::new(temp_dir.path().to_path_buf(), 1000);
        
        let key = "test_operator";
        let bytes = b"compiled_wasm_module";
        
        // Put entry
        let path = cache.put(key, bytes).unwrap();
        assert!(path.exists(), "Cache file should exist");
        
        // Get entry
        let cached_path = cache.get(key);
        assert!(cached_path.is_some(), "Should retrieve cached entry");
        assert_eq!(cached_path.unwrap(), path, "Should return same path");
    }
    
    #[test]
    fn test_operator_cache_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = OperatorCache::new(temp_dir.path().to_path_buf(), 100);
        
        // Add entries that exceed max_bytes
        cache.put("op1", &vec![0u8; 50]).unwrap();
        cache.put("op2", &vec![0u8; 50]).unwrap();
        cache.put("op3", &vec![0u8; 50]).unwrap(); // Should evict op1
        
        // op1 should be evicted
        assert!(cache.get("op1").is_none(), "op1 should be evicted");
        assert!(cache.get("op2").is_some(), "op2 should still be cached");
        assert!(cache.get("op3").is_some(), "op3 should be cached");
    }
    
    #[test]
    fn test_operator_cache_lru() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = OperatorCache::new(temp_dir.path().to_path_buf(), 100);
        
        cache.put("op1", &vec![0u8; 30]).unwrap();
        cache.put("op2", &vec![0u8; 30]).unwrap();
        cache.put("op3", &vec![0u8; 30]).unwrap();
        
        // Access op1 to bump it to end of LRU
        cache.get("op1");
        
        // Add op4, should evict op2 (least recently used)
        cache.put("op4", &vec![0u8; 30]).unwrap();
        
        assert!(cache.get("op1").is_some(), "op1 should still be cached (was accessed)");
        assert!(cache.get("op2").is_none(), "op2 should be evicted (least recently used)");
        assert!(cache.get("op3").is_some(), "op3 should still be cached");
        assert!(cache.get("op4").is_some(), "op4 should be cached");
    }
}

