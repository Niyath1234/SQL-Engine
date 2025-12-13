//! Persistence layer for WorldState (JSON/serde file for now)

use crate::worldstate::WorldState;
use std::fs;
use std::path::{Path, PathBuf};
use anyhow::{Result, Context};

/// Persistence manager for WorldState
pub struct WorldStatePersistence {
    /// Path to the persistence file
    path: PathBuf,
}

impl WorldStatePersistence {
    /// Create a new persistence manager
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
    
    /// Default path (in current directory)
    pub fn default_path() -> PathBuf {
        PathBuf::from(".da_cursor_worldstate.json")
    }
    
    /// Load WorldState from disk
    pub fn load(&self) -> Result<WorldState> {
        if !self.path.exists() {
            // Return empty WorldState if file doesn't exist
            return Ok(WorldState::new());
        }
        
        let content = fs::read_to_string(&self.path)
            .with_context(|| format!("Failed to read WorldState from {}", self.path.display()))?;
        
        let world_state: WorldState = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse WorldState from {}", self.path.display()))?;
        
        Ok(world_state)
    }
    
    /// Save WorldState to disk
    pub fn save(&self, world_state: &WorldState) -> Result<()> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory for {}", self.path.display()))?;
        }
        
        let content = serde_json::to_string_pretty(world_state)
            .with_context(|| "Failed to serialize WorldState")?;
        
        // Write to temporary file first, then rename (atomic write)
        let temp_path = self.path.with_extension("tmp");
        fs::write(&temp_path, content)
            .with_context(|| format!("Failed to write WorldState to {}", temp_path.display()))?;
        
        fs::rename(&temp_path, &self.path)
            .with_context(|| format!("Failed to rename temp file to {}", self.path.display()))?;
        
        Ok(())
    }
    
    /// Check if WorldState file exists
    pub fn exists(&self) -> bool {
        self.path.exists()
    }
    
    /// Get the path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_worldstate.json");
        let persistence = WorldStatePersistence::new(&path);
        
        let mut world_state = WorldState::new();
        world_state.schema_registry.register_table(
            crate::worldstate::schema::TableSchema::new("test_table".to_string())
        );
        
        // Save
        persistence.save(&world_state).unwrap();
        
        // Load
        let loaded = persistence.load().unwrap();
        
        assert_eq!(loaded.schema_registry.has_table("test_table"), true);
    }
}

