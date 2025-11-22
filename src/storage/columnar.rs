use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

/// Columnar batch - represents a batch of rows in columnar format
/// Optimized for SIMD operations and cache locality
#[derive(Clone)]
pub struct ColumnarBatch {
    /// Column arrays (one per column)
    pub columns: Vec<Arc<dyn Array>>,
    
    /// Schema describing the columns
    pub schema: SchemaRef,
    
    /// Number of rows in this batch
    pub row_count: usize,
}

impl ColumnarBatch {
    pub fn new(columns: Vec<Arc<dyn Array>>, schema: SchemaRef) -> Self {
        let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
        
        // PHASE 4: Validate that array types match schema types (dtype-aware validation)
        // This catches type mismatches early
        #[cfg(debug_assertions)]
        {
            let schema_fields = schema.fields();
            if columns.len() != schema_fields.len() {
                eprintln!("  WARNING: Column count ({}) != schema field count ({})", 
                    columns.len(), schema_fields.len());
            }
            
            // Check for duplicate field names in schema
            let field_names: Vec<String> = schema_fields.iter().map(|f| f.name().to_string()).collect();
            let unique_names: std::collections::HashSet<String> = field_names.iter().cloned().collect();
            if field_names.len() != unique_names.len() {
                eprintln!(" ERROR: ColumnarBatch schema has duplicate field names: {:?}", field_names);
                // Find duplicates
                let mut seen = std::collections::HashSet::new();
                let duplicates: Vec<String> = field_names.iter()
                    .filter(|name| !seen.insert(name.clone()))
                    .cloned()
                    .collect();
                eprintln!(" ERROR: Duplicate field names: {:?}", duplicates);
            }
            
            for (i, (col, field)) in columns.iter().zip(schema_fields.iter()).enumerate() {
                let actual_type = col.data_type();
                let expected_type = field.data_type();
                if actual_type != expected_type {
                    eprintln!("  WARNING: Type mismatch for field '{}' (index {}): expected {:?}, got {:?}", 
                        field.name(), i, expected_type, actual_type);
                }
            }
        }
        
        Self {
            columns,
            schema,
            row_count,
        }
    }
    
    /// Get a column by index
    pub fn column(&self, idx: usize) -> Option<&Arc<dyn Array>> {
        self.columns.get(idx)
    }
    
    /// Get a column by name
    pub fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        let idx = self.schema.index_of(name).ok()?;
        self.column(idx)
    }
    
    /// Create an empty batch with the given schema
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            columns: vec![],
            schema,
            row_count: 0,
        }
    }
    
    /// Slice this batch (zero-copy)
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let columns = self
            .columns
            .iter()
            .map(|col| col.slice(offset, length))
            .collect();
        
        Self {
            columns,
            schema: self.schema.clone(),
            row_count: length,
        }
    }
}

/// SIMD-optimized columnar operations
/// Uses compiler auto-vectorization for stable Rust compatibility
pub mod simd_ops {
    /// Vectorized filter - apply predicate to batch (compiler will auto-vectorize)
    pub fn filter_batch<T: Copy>(
        data: &[T],
        predicate: &[bool],
    ) -> Vec<T> {
        // Compiler will auto-vectorize this when optimized
        data.iter()
            .zip(predicate.iter())
            .filter_map(|(val, keep)| if *keep { Some(*val) } else { None })
            .collect()
    }
    
    /// Vectorized aggregation - sum (compiler will auto-vectorize)
    pub fn sum_simd(data: &[f64]) -> f64 {
        // Compiler will auto-vectorize this
        data.iter().sum()
    }
    
    /// Vectorized comparison - compare two arrays (compiler will auto-vectorize)
    pub fn compare_eq_simd<T: Copy + PartialEq>(
        left: &[T],
        right: &[T],
    ) -> Vec<bool> {
        // Compiler will auto-vectorize this
        left.iter()
            .zip(right.iter())
            .map(|(l, r)| l == r)
            .collect()
    }
}

