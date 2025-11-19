use crate::storage::fragment::ColumnFragment;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Version manager for MVCC (Multi-Version Concurrency Control)
pub struct VersionManager {
    current_version: AtomicU64,
}

impl VersionManager {
    pub fn new() -> Self {
        Self {
            current_version: AtomicU64::new(0),
        }
    }
    
    pub fn next_version(&self) -> u64 {
        self.current_version.fetch_add(1, Ordering::SeqCst)
    }
    
    pub fn current_version(&self) -> u64 {
        self.current_version.load(Ordering::SeqCst)
    }
}

/// Versioned fragment - stores multiple versions
pub struct VersionedFragment {
    versions: Vec<(u64, ColumnFragment)>,
}

impl VersionedFragment {
    pub fn new() -> Self {
        Self {
            versions: vec![],
        }
    }
    
    pub fn add_version(&mut self, version: u64, fragment: ColumnFragment) {
        self.versions.push((version, fragment));
    }
    
    pub fn get_version(&self, version: u64) -> Option<&ColumnFragment> {
        self.versions
            .iter()
            .rev()
            .find(|(v, _)| *v <= version)
            .map(|(_, f)| f)
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

