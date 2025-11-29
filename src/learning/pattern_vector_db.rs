/// Persistent vector database for storing learned query patterns
use crate::learning::pattern_extractor::QueryPattern;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use anyhow::Result;

/// Vector database for storing and searching query patterns
pub struct PatternVectorDB {
    /// Pattern metadata
    pub patterns: Vec<QueryPattern>,
    pub pattern_vectors: Vec<Vec<f32>>,
    
    /// File storage paths
    pub db_dir: PathBuf,
    pub patterns_file: PathBuf,
    pub vectors_file: PathBuf,
    
    /// Statistics
    pub total_patterns: usize,
    pub last_updated: u64,
    
    /// Maximum patterns to keep in memory (for performance)
    pub max_memory_patterns: usize,
}

impl PatternVectorDB {
    /// Create new vector database
    pub fn new(db_dir: impl AsRef<Path>) -> Result<Self> {
        let db_dir = db_dir.as_ref();
        std::fs::create_dir_all(db_dir)?;
        
        let patterns_file = db_dir.join("patterns.jsonl");
        let vectors_file = db_dir.join("vectors.bin");
        
        // Load existing patterns if file exists
        let (patterns, vectors) = if patterns_file.exists() {
            Self::load_from_disk(&patterns_file, &vectors_file)?
        } else {
            (Vec::new(), Vec::new())
        };
        
        let total_loaded = patterns.len();
        
        Ok(Self {
            patterns,
            pattern_vectors: vectors,
            db_dir: db_dir.to_path_buf(),
            patterns_file,
            vectors_file,
            total_patterns: total_loaded, // Set from loaded patterns
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            max_memory_patterns: 10000, // Keep last 10K in memory
        })
    }
    
    /// Learn a new pattern (called after query execution)
    pub fn learn_pattern(
        &mut self,
        pattern: QueryPattern,
        embedding: Vec<f32>,
    ) -> Result<()> {
        // 1. Add to in-memory storage
        self.patterns.push(pattern);
        self.pattern_vectors.push(embedding.clone());
        self.total_patterns += 1;
        
        // 2. Persist to disk (append to file)
        self.append_pattern_to_disk(&self.patterns[self.patterns.len() - 1], &embedding)?;
        
        // 3. Trim memory if too many patterns
        if self.patterns.len() > self.max_memory_patterns {
            // Keep only recent patterns in memory
            let remove_count = self.patterns.len() - self.max_memory_patterns;
            self.patterns.drain(0..remove_count);
            self.pattern_vectors.drain(0..remove_count);
        }
        
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        
        Ok(())
    }
    
    /// Find similar patterns (basic cosine similarity, Phase 2 will use HNSW)
    pub fn find_similar_patterns(
        &self,
        query_embedding: &[f32],
        top_k: usize,
    ) -> Result<Vec<(usize, f32)>> {
        let mut similarities: Vec<(usize, f32)> = Vec::new();
        
        // Compute cosine similarity with all patterns
        for (idx, pattern_vec) in self.pattern_vectors.iter().enumerate() {
            if pattern_vec.len() != query_embedding.len() {
                continue; // Skip mismatched dimensions
            }
            
            let similarity = cosine_similarity(query_embedding, pattern_vec);
            similarities.push((idx, similarity));
        }
        
        // Sort by similarity (highest first) and take top-k
        similarities.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        if similarities.len() > top_k {
            similarities.truncate(top_k);
        }
        
        Ok(similarities)
    }
    
    /// Append pattern to disk (JSON Lines format)
    fn append_pattern_to_disk(
        &self,
        pattern: &QueryPattern,
        embedding: &[f32],
    ) -> Result<()> {
        use std::io::Write;
        
        // Append to patterns file (JSON Lines)
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.patterns_file)?;
        
        let json = serde_json::to_string(pattern)?;
        writeln!(file, "{}", json)?;
        file.flush()?;
        
        // Append embedding to binary file
        let mut vec_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.vectors_file)?;
        
        // Write: [dim: u32][values: f32*dim]
        vec_file.write_all(&(embedding.len() as u32).to_le_bytes())?;
        for &val in embedding {
            vec_file.write_all(&val.to_le_bytes())?;
        }
        vec_file.flush()?;
        
        Ok(())
    }
    
    /// Load patterns from disk
    fn load_from_disk(
        patterns_file: &Path,
        vectors_file: &Path,
    ) -> Result<(Vec<QueryPattern>, Vec<Vec<f32>>)> {
        // Load patterns from JSON Lines (only recent ones for performance)
        let patterns = if patterns_file.exists() {
            let lines: Vec<String> = std::fs::read_to_string(patterns_file)?
                .lines()
                .map(|s| s.to_string())
                .collect();
            
            // Load only recent patterns (last N) for memory efficiency
            let recent_lines = if lines.len() > 10000 {
                lines.iter().skip(lines.len() - 10000).cloned().collect()
            } else {
                lines
            };
            
            recent_lines
                .iter()
                .filter_map(|line| serde_json::from_str::<QueryPattern>(line).ok())
                .collect()
        } else {
            Vec::new()
        };
        
        // Load vectors from binary file (matching recent patterns)
        let vectors = if vectors_file.exists() && !patterns.is_empty() {
            // Only load vectors for patterns we loaded
            let mut loaded_vectors = Vec::new();
            if let Ok(data) = std::fs::read(vectors_file) {
                // Try to load all vectors, but keep only recent ones
                if let Ok(all_vectors) = Self::deserialize_vectors(&data) {
                    if all_vectors.len() > 10000 {
                        // Keep only recent vectors
                        let skip_count = all_vectors.len() - 10000;
                        loaded_vectors = all_vectors
                            .into_iter()
                            .skip(skip_count)
                            .collect();
                    } else {
                        loaded_vectors = all_vectors;
                    }
                }
            }
            loaded_vectors
        } else {
            Vec::new()
        };
        
        Ok((patterns, vectors))
    }
    
    /// Deserialize vectors from binary format
    fn deserialize_vectors(data: &[u8]) -> Result<Vec<Vec<f32>>> {
        let mut vectors = Vec::new();
        let mut offset = 0;
        
        while offset + 4 <= data.len() {
            // Read dimension
            let dim = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;
            
            // Check if we have enough data for the vector
            let vector_bytes = dim * 4;
            if offset + vector_bytes > data.len() {
                break; // Incomplete vector
            }
            
            // Read vector values
            let mut vec = Vec::with_capacity(dim);
            for _ in 0..dim {
                let val = f32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                vec.push(val);
                offset += 4;
            }
            vectors.push(vec);
        }
        
        Ok(vectors)
    }
    
    /// Get recent patterns (for context)
    pub fn get_recent_patterns(&self, count: usize) -> Vec<&QueryPattern> {
        let start = if self.patterns.len() > count {
            self.patterns.len() - count
        } else {
            0
        };
        self.patterns.iter().skip(start).collect()
    }
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a > 0.0 && norm_b > 0.0 {
        dot_product / (norm_a * norm_b)
    } else {
        0.0
    }
}

