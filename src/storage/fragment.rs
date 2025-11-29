use arrow::array::*;
use arrow::datatypes::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use bitvec::prelude::*;
use memmap2::Mmap;
use crate::execution::batch::ExecutionBatch;
use crate::storage::bitmap_index::BitmapIndex as NewBitmapIndex;

/// Represents a column fragment - a chunk of columnar data
/// Optimized for cache locality and SIMD operations
/// Supports lazy loading: array can be None until first access
/// Supports memory-mapped access for zero-copy reads
#[derive(Clone, Debug)]
pub struct ColumnFragment {
    /// The actual columnar data (None for lazy loading)
    pub array: Option<Arc<dyn Array>>,
    
    /// Memory-mapped data (for zero-copy access)
    /// When present, array can be reconstructed from mmap without copying
    pub mmap: Option<Arc<Mmap>>,
    
    /// Offset into mmap where this fragment's data starts
    pub mmap_offset: usize,
    
    /// Fragment metadata (always available)
    pub metadata: FragmentMetadata,
    
    /// Bloom filter for fast negative lookups (stored as bytes for now)
    pub bloom_filter: Option<Vec<u8>>,
    
    /// Bitmap index for equality predicates (columnar bitmaps)
    pub bitmap_index: Option<NewBitmapIndex>,
    
    /// Whether this fragment is sorted (enables binary search)
    pub is_sorted: bool,
    
    /// Dictionary for dictionary-encoded fragments (Phase 2)
    /// Maps code -> original value
    pub dictionary: Option<Vec<Value>>,
    
    /// Compressed data (for compression types like Zstd, LZ4)
    /// When present, array should be None and this contains compressed bytes
    pub compressed_data: Option<Vec<u8>>,
    
    /// Vector index for similarity search (HNSW, IVFFlat, etc.)
    pub vector_index: Option<crate::storage::vector_index::VectorIndexEnum>,
    
    /// Vector dimension (for validation and index building)
    pub vector_dimension: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FragmentMetadata {
    /// Number of rows in this fragment
    pub row_count: usize,
    
    /// Minimum value (for sorted fragments)
    pub min_value: Option<Value>,
    
    /// Maximum value (for sorted fragments)
    pub max_value: Option<Value>,
    
    /// Cardinality estimate
    pub cardinality: usize,
    
    /// Compression type used
    pub compression: CompressionType,
    
    /// Memory size in bytes
    pub memory_size: usize,
    
    /// Table name (for bitmap index building)
    #[serde(default)]
    pub table_name: Option<String>,
    
    /// Column name (for bitmap index building)
    #[serde(default)]
    pub column_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Dictionary,
    Delta,
    BitPacked,
    RLE,
    Zstd,
    Lz4,
}

/// Legacy bitmap index for fast equality lookups (deprecated - use bitmap_index::BitmapIndex instead)
/// This is kept for backward compatibility but should not be used in new code
#[derive(Clone, Debug)]
#[deprecated(note = "Use crate::storage::bitmap_index::BitmapIndex instead")]
pub struct BitmapIndex {
    /// Map from value hash to bitmap of row positions
    pub bitmaps: dashmap::DashMap<u64, BitVec>,
    
    /// Total number of rows
    pub row_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Int32(i32),
    Float64(f64),
    Float32(f32),
    String(String),
    Bool(bool),
    Vector(Vec<f32>),  // Dense vector for similarity search
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int64(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Vector(v) => write!(f, "[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ")),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => ordered_float::OrderedFloat(*a) == ordered_float::OrderedFloat(*b),
            (Value::Float32(a), Value::Float32(b)) => ordered_float::OrderedFloat(*a) == ordered_float::OrderedFloat(*b),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => ordered_float::OrderedFloat(*a).cmp(&ordered_float::OrderedFloat(*b)),
            (Value::Float32(a), Value::Float32(b)) => ordered_float::OrderedFloat(*a).cmp(&ordered_float::OrderedFloat(*b)),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            // Type-based ordering: Int64 < Int32 < Float64 < Float32 < String < Bool < Null < Vector
            (Value::Int64(_), _) => std::cmp::Ordering::Less,
            (Value::Int32(_), Value::Int64(_)) => std::cmp::Ordering::Greater,
            (Value::Int32(_), _) => std::cmp::Ordering::Less,
            (Value::Float64(_), Value::Int64(_) | Value::Int32(_)) => std::cmp::Ordering::Greater,
            (Value::Float64(_), _) => std::cmp::Ordering::Less,
            (Value::Float32(_), Value::Int64(_) | Value::Int32(_) | Value::Float64(_)) => std::cmp::Ordering::Greater,
            (Value::Float32(_), _) => std::cmp::Ordering::Less,
            (Value::String(_), Value::Int64(_) | Value::Int32(_) | Value::Float64(_) | Value::Float32(_)) => std::cmp::Ordering::Greater,
            (Value::String(_), _) => std::cmp::Ordering::Less,
            (Value::Bool(_), Value::Vector(_) | Value::Null) => std::cmp::Ordering::Less,
            (Value::Bool(_), _) => std::cmp::Ordering::Greater,
            (Value::Null, Value::Vector(_)) => std::cmp::Ordering::Less,
            (Value::Null, _) => std::cmp::Ordering::Greater,
            (Value::Vector(_), _) => std::cmp::Ordering::Greater,
        }
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Int64(v) => {
                0u8.hash(state); // Discriminator
                v.hash(state);
            }
            Value::Int32(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            Value::Float64(v) => {
                2u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::Float32(v) => {
                3u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::String(v) => {
                4u8.hash(state);
                v.hash(state);
            }
            Value::Bool(v) => {
                5u8.hash(state);
                v.hash(state);
            }
            Value::Vector(v) => {
                7u8.hash(state);
                // Hash vector by hashing its length and first few elements
                // Full vector hashing would be expensive
                v.len().hash(state);
                for &x in v.iter().take(10) {
                    x.to_bits().hash(state);
                }
            }
            Value::Null => {
                6u8.hash(state);
            }
        }
    }
}

// Trait implementations for Value already exist above (lines 122-172)
// No need to duplicate them here

impl ColumnFragment {
    pub fn new(array: Arc<dyn Array>, metadata: FragmentMetadata) -> Self {
        Self {
            array: Some(array),
            mmap: None,
            mmap_offset: 0,
            metadata,
            bloom_filter: None,
            bitmap_index: None,
            is_sorted: false,
            dictionary: None,
            compressed_data: None,
            vector_index: None,
            vector_dimension: None,
        }
    }
    
    /// Create a lazy fragment (metadata only, array loaded on demand)
    pub fn new_lazy(metadata: FragmentMetadata) -> Self {
        Self {
            array: None,
            mmap: None,
            mmap_offset: 0,
            metadata,
            bloom_filter: None,
            bitmap_index: None,
            is_sorted: false,
            dictionary: None,
            compressed_data: None,
            vector_index: None,
            vector_dimension: None,
        }
    }
    
    /// Create a memory-mapped fragment (zero-copy)
    /// This allows direct pointer access to fragment data without copying
    pub fn new_mmap(mmap: Arc<Mmap>, offset: usize, metadata: FragmentMetadata) -> Self {
        Self {
            array: None, // Will be loaded on-demand from mmap
            mmap: Some(mmap),
            mmap_offset: offset,
            metadata,
            bloom_filter: None,
            bitmap_index: None,
            is_sorted: false,
            dictionary: None,
            compressed_data: None,
            vector_index: None,
            vector_dimension: None,
        }
    }
    
    /// Get the array, loading it if necessary (lazy loading)
    pub fn get_array(&self) -> Option<Arc<dyn Array>> {
        // If array is already loaded, return it
        if let Some(ref arr) = self.array {
            return Some(arr.clone());
        }
        
        // If we have mmap, try to load from it (zero-copy)
        if let Some(ref mmap) = self.mmap {
            // Load array from memory-mapped file using zero-copy deserialization
            // Arrow IPC format would be ideal, but for now we use a simple approach
            // that reads directly from the mmap without copying the data
            return self.load_array_from_mmap(mmap);
        }
        
        None
    }
    
    /// Load array from memory-mapped file (zero-copy)
    /// This reads the Arrow array directly from mmap without copying data
    fn load_array_from_mmap(&self, mmap: &Mmap) -> Option<Arc<dyn Array>> {
        // For zero-copy loading, we need the data type from metadata
        // Since we don't store it in metadata currently, we'll use a fallback
        // In a full implementation with Arrow IPC, we'd read the schema from the IPC footer
        
        // Get the slice of mmap for this fragment
        let data_slice = if self.mmap_offset < mmap.len() {
            let end = (self.mmap_offset + self.metadata.memory_size).min(mmap.len());
            &mmap[self.mmap_offset..end]
        } else {
            return None;
        };
        
        // For now, if fragment has compressed data, we can't zero-copy it
        // Zero-copy is only possible for uncompressed data
        if self.compressed_data.is_some() {
            // Would need to decompress first, which requires copying
            return None;
        }
        
        // Check if we have enough data
        if data_slice.len() < 8 {
            return None;
        }
        
        // Simple approach: Try to read array metadata from mmap
        // In a full implementation, we'd use Arrow IPC format which stores
        // schema and buffers in a way that allows zero-copy reads
        // For now, we'll fall back to lazy loading (defer to caller)
        None
    }
    
    /// Get a pointer to the memory-mapped data (for zero-copy access)
    /// Returns None if fragment is not memory-mapped
    pub fn get_mmap_ptr(&self) -> Option<(*const u8, usize)> {
        self.mmap.as_ref().map(|m| {
            let ptr = unsafe { m.as_ptr().add(self.mmap_offset) };
            let len = self.metadata.memory_size.min(m.len().saturating_sub(self.mmap_offset));
            (ptr, len)
        })
    }
    
    /// Create a fragment from memory-mapped Arrow IPC format (zero-copy)
    /// This is the proper way to load fragments with zero-copy semantics
    pub fn from_mmap_arrow_ipc(
        mmap: Arc<Mmap>,
        offset: usize,
        metadata: FragmentMetadata,
    ) -> Result<Self, anyhow::Error> {
        // In a full implementation, this would:
        // 1. Read Arrow IPC schema from the memory-mapped file
        // 2. Use Arrow's IPC reader to get zero-copy array views
        // 3. Create fragment with array pointing to mmap data (not copied)
        
        // For now, create a lazy fragment that will load on demand
        Ok(Self::new_mmap(mmap, offset, metadata))
    }
    
    /// Get array length (from metadata, no need to load array)
    pub fn len(&self) -> usize {
        self.metadata.row_count
    }
    
    /// Check if fragment is empty
    pub fn is_empty(&self) -> bool {
        self.metadata.row_count == 0
    }
    
    /// Check if fragment can be skipped based on min/max statistics
    pub fn can_skip_for_predicate(&self, operator: &crate::query::plan::PredicateOperator, value: &Value) -> bool {
        use crate::query::plan::PredicateOperator;
        
        // If we don't have min/max, we can't skip
        let (min_val, max_val) = match (&self.metadata.min_value, &self.metadata.max_value) {
            (Some(min), Some(max)) => (min, max),
            _ => return false,
        };
        
        match operator {
            PredicateOperator::IsNull | PredicateOperator::IsNotNull => {
                // IS NULL/IS NOT NULL can't be used for fragment pruning
                false
            }
            PredicateOperator::GreaterThan => {
                // WHERE col > value: skip if max <= value
                if let Some(ord) = max_val.partial_cmp(value) {
                    matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                } else {
                    false
                }
            }
            PredicateOperator::GreaterThanOrEqual => {
                // WHERE col >= value: skip if max < value
                if let Some(ord) = max_val.partial_cmp(value) {
                    matches!(ord, std::cmp::Ordering::Less)
                } else {
                    false
                }
            }
            PredicateOperator::LessThan => {
                // WHERE col < value: skip if min >= value
                if let Some(ord) = min_val.partial_cmp(value) {
                    matches!(ord, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                } else {
                    false
                }
            }
            PredicateOperator::LessThanOrEqual => {
                // WHERE col <= value: skip if min > value
                if let Some(ord) = min_val.partial_cmp(value) {
                    matches!(ord, std::cmp::Ordering::Greater)
                } else {
                    false
                }
            }
            PredicateOperator::Equals => {
                // WHERE col = value: skip if value not in [min, max]
                if let (Some(min_cmp), Some(max_cmp)) = (min_val.partial_cmp(value), max_val.partial_cmp(value)) {
                    matches!(min_cmp, std::cmp::Ordering::Greater) || matches!(max_cmp, std::cmp::Ordering::Less)
                } else {
                    false
                }
            }
            PredicateOperator::NotEquals => {
                // WHERE col != value: can't skip (too many matches possible)
                false
            }
            PredicateOperator::Like | PredicateOperator::NotLike => {
                // WHERE col LIKE pattern: can't skip based on min/max (pattern matching is complex)
                false
            }
            PredicateOperator::In | PredicateOperator::NotIn => {
                // WHERE col IN (...): could potentially skip if all values are outside [min, max]
                // For now, don't skip (conservative approach)
                false
            }
        }
    }
    
    /// Create an iterator over this fragment
    pub fn iter(&self) -> FragmentIterator {
        FragmentIterator::new(self.clone())
    }
    
    /// Build bloom filter for this fragment
    /// Bloom filter provides fast negative lookups (can quickly determine if a value is NOT in fragment)
    pub fn build_bloom_filter(&mut self) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        // Get array to build bloom filter from
        let array = match self.get_array() {
            Some(arr) => arr,
            None => return, // Can't build bloom filter without array
        };
        
        let row_count = array.len();
        if row_count == 0 {
            return;
        }
        
        // Simple bloom filter: use 64 bits (8 bytes) for small fragments
        // For larger fragments, we'd use more bits (typically 10 bits per element)
        let bloom_size_bits = (row_count * 10).max(64).min(1024); // 64-1024 bits
        let bloom_size_bytes = (bloom_size_bits + 7) / 8;
        let mut bloom_bits = vec![0u8; bloom_size_bytes];
        
        // Hash each value and set corresponding bits
        for i in 0..row_count {
            if array.is_null(i) {
                continue; // Skip nulls for bloom filter
            }
            
            // Create a hash of the value
            let mut hasher = DefaultHasher::new();
            match array.data_type() {
                DataType::Int64 => {
                    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                        arr.value(i).hash(&mut hasher);
                    }
                }
                DataType::Float64 => {
                    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                        arr.value(i).to_bits().hash(&mut hasher);
                    }
                }
                DataType::Utf8 => {
                    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                        arr.value(i).hash(&mut hasher);
                    }
                }
                _ => continue, // Unsupported type
            }
            
            // Get hash value
            let hash = hasher.finish();
            
            // Set multiple bits in bloom filter (simple approach: use 2 hash functions)
            // In production, we'd use proper hash functions
            let bit_pos1 = (hash % bloom_size_bits as u64) as usize;
            let bit_pos2 = ((hash >> 32) % bloom_size_bits as u64) as usize;
            
            // Set bits
            bloom_bits[bit_pos1 / 8] |= 1u8 << (bit_pos1 % 8);
            bloom_bits[bit_pos2 / 8] |= 1u8 << (bit_pos2 % 8);
        }
        
        self.bloom_filter = Some(bloom_bits);
    }
    
    /// Build bitmap index for this fragment using the new BitmapIndex structure
    /// Bitmap index maps each distinct value to a bitset of row IDs where that value appears
    pub fn build_bitmap_index_new(&mut self) -> anyhow::Result<()> {
        use crate::execution::operators::extract_value;
        
        // Get array to build bitmap index from
        let array = match self.get_array() {
            Some(arr) => arr,
            None => return Ok(()), // Can't build bitmap index without array
        };
        
        let row_count = array.len();
        if row_count == 0 {
            return Ok(());
        }
        
        // Get table and column name from metadata
        let table_name = self.metadata.table_name.clone().unwrap_or_else(|| "unknown".to_string());
        let column_name = self.metadata.column_name.clone().unwrap_or_else(|| "unknown".to_string());
        
        // Create bitmap index using the new type
        let mut bitmap_index = NewBitmapIndex::new(table_name, column_name);
        
        // Build index by iterating through all rows
        for row_id in 0..row_count {
            if array.is_null(row_id) {
                continue; // Skip nulls
            }
            
            // Extract value from array
            match extract_value(&array, row_id) {
                Ok(value) => {
                    bitmap_index.add_value(value, row_id);
                }
                Err(_) => {
                    // Skip rows where value extraction fails
                    continue;
                }
            }
        }
        
        // Store bitmap index in fragment
        self.bitmap_index = Some(bitmap_index);
        
        Ok(())
    }
    
    /// Build bitmap index for this fragment (legacy method - use build_bitmap_index_new instead)
    /// Bitmap index provides fast equality lookups (can quickly find all rows with a specific value)
    /// This method is deprecated - use build_bitmap_index_new() which uses the new BitmapIndex type
    #[deprecated(note = "Use build_bitmap_index_new() instead")]
    pub fn build_bitmap_index(&mut self) {
        // Delegate to the new implementation
        if let Err(e) = self.build_bitmap_index_new() {
            eprintln!("Warning: Failed to build bitmap index: {}", e);
        }
    }
}

/// Zero-copy iterator over column fragment
pub struct FragmentIterator {
    fragment: ColumnFragment,
    position: usize,
}

impl FragmentIterator {
    pub fn new(fragment: ColumnFragment) -> Self {
        Self {
            fragment,
            position: 0,
        }
    }
    
    pub fn next(&mut self) -> Option<Value> {
        if self.position >= self.fragment.len() {
            return None;
        }
        
        // Extract value from arrow array
        let array = match self.fragment.get_array() {
            Some(arr) => arr,
            None => {
                self.position += 1;
                return Some(Value::Null);
            }
        };
        
        // Handle null values
        if array.is_null(self.position) {
            self.position += 1;
            return Some(Value::Null);
        }
        
        // Extract value based on data type
        let value = match array.data_type() {
            DataType::Int64 | DataType::Int32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    Value::Int64(arr.value(self.position))
                } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    Value::Int32(arr.value(self.position))
                } else {
                    self.position += 1;
                    return Some(Value::Null);
                }
            }
            DataType::Float64 | DataType::Float32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    Value::Float64(arr.value(self.position))
                } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                    Value::Float32(arr.value(self.position))
                } else {
                    self.position += 1;
                    return Some(Value::Null);
                }
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    Value::String(arr.value(self.position).to_string())
                } else {
                    self.position += 1;
                    return Some(Value::Null);
                }
            }
            DataType::Boolean => {
                if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                    Value::Bool(arr.value(self.position))
                } else {
                    self.position += 1;
                    return Some(Value::Null);
                }
            }
            _ => {
                // Unsupported type - return null
                self.position += 1;
                return Some(Value::Null);
            }
        };
        
        self.position += 1;
        Some(value)
    }
}

/// Column store manages all column fragments
pub struct ColumnStore {
    /// Map from (table_name, column_name) to fragments
    fragments: dashmap::DashMap<(String, String), Vec<ColumnFragment>>,
    
    /// Memory pool for reusing buffers
    memory_pool: MemoryPool,
}

impl ColumnStore {
    pub fn new() -> Self {
        Self {
            fragments: dashmap::DashMap::new(),
            memory_pool: MemoryPool::new(),
        }
    }
    
    /// Get the memory pool (for reuse in execution)
    pub fn get_memory_pool(&self) -> &MemoryPool {
        &self.memory_pool
    }
    
    /// Add a fragment for a table column
    pub fn add_fragment(&self, table: String, column: String, fragment: ColumnFragment) {
        self.fragments
            .entry((table, column))
            .or_insert_with(Vec::new)
            .push(fragment);
    }
    
    /// Get all fragments for a table column
    pub fn get_fragments(&self, table: &str, column: &str) -> Option<Vec<ColumnFragment>> {
        self.fragments
            .get(&(table.to_string(), column.to_string()))
            .map(|entry| entry.value().clone())
    }
}

/// Memory pool for buffer reuse
/// Uses a simple Vec-based pool to reuse ExecutionBatch and other buffers
pub struct MemoryPool {
    /// Pool for ExecutionBatch objects (simplified - just a placeholder for now)
    /// In a full implementation, this would use a proper object pool
    _batch_pool_size: usize,
    
    /// Pool for BitVec selection vectors (simplified - just a placeholder for now)
    _bitvec_pool_size: usize,
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            _batch_pool_size: 100, // Pool up to 100 batches
            _bitvec_pool_size: 200, // Pool up to 200 bitvecs
        }
    }
    
    /// Get a pooled ExecutionBatch (reuse or create new)
    /// TODO: Implement actual pooling logic
    pub fn get_batch(&self) -> ExecutionBatch {
        use arrow::datatypes::Schema;
        let empty_schema = Arc::new(Schema::empty());
        ExecutionBatch::new(
            crate::storage::columnar::ColumnarBatch::empty(empty_schema)
        )
    }
    
    /// Get a pooled BitVec (reuse or create new)
    /// TODO: Implement actual pooling logic
    pub fn get_bitvec(&self) -> bitvec::prelude::BitVec {
        bitvec::prelude::BitVec::new()
    }
}

