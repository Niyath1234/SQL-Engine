//! 3-Tier Storage Architecture
//!
//! Level 1: Persistent Hypergraph (128-bit vectorized, never cleared)
//!   - Stores metadata, paths, and statistics
//!   - Optimized for minimal storage footprint
//!   - Builds up over time, persists across restarts
//!
//! Level 2: Session Cache (larger, refreshed per session)
//!   - Session-scoped query results and plans
//!   - Promotes key patterns to Level 1
//!
//! Level 3: User Pattern Map (client-side)
//!   - Table hotness tracking
//!   - Query pattern analysis
//!   - Enables pre-planning based on user behavior

pub mod persistent_hypergraph;
pub mod session_cache;
pub mod user_patterns;

pub use persistent_hypergraph::PersistentHypergraph;
pub use session_cache::SessionCache;
pub use user_patterns::UserPatternMap;

use std::sync::{Arc, Mutex};
use crate::hypergraph::HyperGraph;

/// 3-tier storage manager
pub struct TieredStorage {
    /// Level 1: Persistent hypergraph (128-bit optimized, thread-safe)
    pub persistent: Arc<Mutex<PersistentHypergraph>>,
    
    /// Level 2: Session cache (promotes to Level 1)
    pub session: SessionCache,
    
    /// Level 3: User pattern map (client-side, loaded on demand)
    pub user_patterns: Option<Arc<UserPatternMap>>,
    
    /// Main hypergraph (in-memory working copy)
    pub graph: Arc<HyperGraph>,
}

impl TieredStorage {
    pub fn new(graph: Arc<HyperGraph>) -> Result<Self, anyhow::Error> {
        let persistent = Arc::new(Mutex::new(PersistentHypergraph::load_or_create()?));
        
        Ok(Self {
            persistent,
            session: SessionCache::new(),
            user_patterns: None,
            graph,
        })
    }
    
    /// Load user patterns from device storage
    pub fn load_user_patterns(&mut self, user_id: &str) -> Result<(), anyhow::Error> {
        let patterns = UserPatternMap::load_from_device(user_id)?;
        self.user_patterns = Some(Arc::new(patterns));
        Ok(())
    }
    
    /// Save persistent hypergraph to disk
    pub fn save_persistent(&self) -> Result<(), anyhow::Error> {
        let persistent = self.persistent.lock().unwrap();
        persistent.save()?;
        Ok(())
    }
    
    /// Merge in-memory hypergraph into persistent storage
    pub fn merge_graph_to_persistent(&self) -> Result<(), anyhow::Error> {
        let mut persistent = self.persistent.lock().unwrap();
        persistent.merge_from_graph(&self.graph);
        Ok(())
    }
    
    /// Promote session cache items to persistent storage
    pub fn promote_session_to_persistent(&mut self) -> Result<(), anyhow::Error> {
        // Get promotion candidates from session cache
        let candidates = self.session.get_promotion_candidates();
        
        // Add promoted paths to persistent storage
        let mut persistent = self.persistent.lock().unwrap();
        for (signature, path) in candidates {
            persistent.add_path(&signature, &path);
        }
        
        Ok(())
    }
    
    /// Get persistent hypergraph (for read-only access)
    pub fn get_persistent(&self) -> std::sync::MutexGuard<'_, PersistentHypergraph> {
        self.persistent.lock().unwrap()
    }
}

