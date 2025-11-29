/// Enhanced ColumnSchema for DuckDB/Presto-grade join engine
/// 
/// This structure provides robust column resolution using stable ColumnIds.
/// All column resolution is ColumnId-driven, not index-driven.
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::DataType;
use super::column_id::ColumnId;

/// Column Schema with ColumnId-driven resolution
/// 
/// This structure stores:
/// - Vec<ColumnId> - Ordered list of column IDs (determines physical order)
/// - Vec<qualified_name> - Fully qualified names (e.g., "table.column")
/// - Vec<unqualified_name> - Unqualified names (e.g., "column")
/// - HashMap<String, ColumnId> qualified_lookup - Fast qualified name lookup
/// - HashMap<String, Vec<ColumnId>> unqualified_lookup - Unqualified name lookup (handles ambiguity)
/// - HashMap<ColumnId, usize> physical_index - ColumnId to physical index mapping
/// 
/// Key invariants:
/// - All column resolution is ColumnId-driven (not index-driven)
/// - ColumnIds are NEVER modified after creation
/// - Qualified names always resolve to exactly one ColumnId
/// - Unqualified names may resolve to multiple ColumnIds (ambiguity)
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Ordered list of ColumnIds (determines physical column order)
    /// This maps 1:1 to Arrow schema fields and physical array indices
    column_ids: Vec<ColumnId>,
    
    /// Qualified names (one per ColumnId, in column_ids order)
    /// Format: "table.column" or "alias.column"
    qualified_names: Vec<String>,
    
    /// Unqualified names (one per ColumnId, in column_ids order)
    /// Format: "column"
    unqualified_names: Vec<String>,
    
    /// Data types (one per ColumnId, in column_ids order)
    data_types: Vec<DataType>,
    
    /// Fast lookup: qualified_name → ColumnId
    /// Qualified names always map to exactly one ColumnId
    qualified_lookup: HashMap<String, ColumnId>,
    
    /// Unqualified name lookup: unqualified_name → Vec<ColumnId>
    /// May contain multiple ColumnIds for ambiguous unqualified names
    unqualified_lookup: HashMap<String, Vec<ColumnId>>,
    
    /// ColumnId → physical_index mapping
    /// Physical index is the position in the Arrow array/ColumnarBatch
    physical_index: HashMap<ColumnId, usize>,
}

/// Reference-counted ColumnSchema for sharing across operators
pub type SchemaRef = Arc<ColumnSchema>;

impl ColumnSchema {
    /// Create new empty schema
    pub fn new() -> Self {
        Self {
            column_ids: Vec::new(),
            qualified_names: Vec::new(),
            unqualified_names: Vec::new(),
            data_types: Vec::new(),
            qualified_lookup: HashMap::new(),
            unqualified_lookup: HashMap::new(),
            physical_index: HashMap::new(),
        }
    }
    
    /// Add a column with both qualified and unqualified names
    /// 
    /// # Arguments
    /// * `column_id` - Stable ColumnId (table_id, column_ordinal)
    /// * `qualified_name` - Fully qualified name: "table.column" or "alias.column"
    /// * `unqualified_name` - Unqualified name: "column"
    /// * `data_type` - Arrow data type
    /// 
    /// # Rules
    /// - ColumnId is NEVER modified after creation
    /// - Qualified name always added to qualified_lookup
    /// - Unqualified name added to unqualified_lookup (may be ambiguous)
    /// - Physical index is automatically assigned based on insertion order
    pub fn add_column(
        &mut self,
        column_id: ColumnId,
        qualified_name: String,
        unqualified_name: String,
        data_type: DataType,
    ) {
        let physical_idx = self.column_ids.len();
        
        // Add to ordered lists
        self.column_ids.push(column_id);
        self.qualified_names.push(qualified_name.clone());
        self.unqualified_names.push(unqualified_name.clone());
        self.data_types.push(data_type);
        
        // Add to qualified lookup (always unambiguous)
        self.qualified_lookup.insert(qualified_name, column_id);
        
        // Add to unqualified lookup (may be ambiguous - store all matches)
        self.unqualified_lookup
            .entry(unqualified_name)
            .or_insert_with(Vec::new)
            .push(column_id);
        
        // Add to physical index mapping
        self.physical_index.insert(column_id, physical_idx);
    }
    
    /// Resolve qualified name to ColumnId
    /// 
    /// # Returns
    /// - Some(ColumnId) if exactly one match found
    /// - None if not found
    /// 
    /// Qualified names are always unambiguous.
    pub fn resolve_qualified(&self, qualified: &str) -> Option<ColumnId> {
        self.qualified_lookup.get(qualified).copied()
    }
    
    /// Resolve unqualified name to ColumnId
    /// 
    /// # Returns
    /// - Ok(ColumnId) if exactly one match found
    /// - Err(String) if ambiguous (multiple matches) or not found
    pub fn resolve_unqualified(&self, name: &str) -> Result<ColumnId, String> {
        // Check if it's actually a qualified name
        if name.contains('.') {
            return self.resolve_qualified(name)
                .ok_or_else(|| format!("Column '{}' not found", name));
        }
        
        match self.unqualified_lookup.get(name) {
            Some(matches) => {
                match matches.len() {
                    0 => Err(format!("Column '{}' not found", name)),
                    1 => Ok(matches[0]),
                    _ => Err(format!(
                        "Ambiguous column reference '{}': found {} matches. Use qualified name (e.g., 'table.{}').",
                        name, matches.len(), name
                    )),
                }
            }
            None => Err(format!("Column '{}' not found", name)),
        }
    }
    
    /// Get physical index for ColumnId
    /// 
    /// Physical index is the position in the Arrow array/ColumnarBatch.
    /// 
    /// # Returns
    /// - Some(usize) if ColumnId found
    /// - None if ColumnId not in schema
    pub fn physical_index(&self, column_id: &ColumnId) -> Option<usize> {
        self.physical_index.get(column_id).copied()
    }
    
    /// Get qualified name for ColumnId
    pub fn qualified_name(&self, column_id: &ColumnId) -> Option<&String> {
        self.column_ids
            .iter()
            .position(|&id| id == *column_id)
            .and_then(|idx| self.qualified_names.get(idx))
    }
    
    /// Get unqualified name for ColumnId
    pub fn unqualified_name(&self, column_id: &ColumnId) -> Option<&String> {
        self.column_ids
            .iter()
            .position(|&id| id == *column_id)
            .and_then(|idx| self.unqualified_names.get(idx))
    }
    
    /// Get data type for ColumnId
    pub fn data_type(&self, column_id: &ColumnId) -> Option<&DataType> {
        self.column_ids
            .iter()
            .position(|&id| id == *column_id)
            .and_then(|idx| self.data_types.get(idx))
    }
    
    /// Get ColumnId at physical index
    pub fn column_id_at(&self, index: usize) -> Option<ColumnId> {
        self.column_ids.get(index).copied()
    }
    
    /// Get number of columns
    pub fn len(&self) -> usize {
        self.column_ids.len()
    }
    
    /// Check if schema is empty
    pub fn is_empty(&self) -> bool {
        self.column_ids.is_empty()
    }
    
    /// Get all ColumnIds (in physical order)
    pub fn column_ids(&self) -> &[ColumnId] {
        &self.column_ids
    }
    
    /// Get all qualified names (in physical order)
    pub fn qualified_names(&self) -> &[String] {
        &self.qualified_names
    }
    
    /// Get all unqualified names (in physical order)
    pub fn unqualified_names(&self) -> &[String] {
        &self.unqualified_names
    }
    
    /// Get all data types (in physical order)
    pub fn data_types(&self) -> &[DataType] {
        &self.data_types
    }
    
    /// Merge two schemas (for JOIN)
    /// 
    /// # Rules
    /// - Preserves ALL ColumnIds from both schemas (never reuses or regenerates)
    /// - Merged schema = left.columns + right.columns (in order)
    /// - Qualified names prevent conflicts
    /// - Unqualified names are checked for ambiguity
    /// 
    /// # Returns
    /// Merged schema with all columns from both sides
    pub fn merge(left: &Self, right: &Self) -> Result<Self, String> {
        let mut merged = Self::new();
        
        // Add all left columns (preserve ColumnIds)
        for (idx, &column_id) in left.column_ids.iter().enumerate() {
            let qualified = left.qualified_names[idx].clone();
            let unqualified = left.unqualified_names[idx].clone();
            let data_type = left.data_types[idx].clone();
            merged.add_column(column_id, qualified, unqualified, data_type);
        }
        
        // Add all right columns (preserve ColumnIds)
        for (idx, &column_id) in right.column_ids.iter().enumerate() {
            let qualified = right.qualified_names[idx].clone();
            let unqualified = right.unqualified_names[idx].clone();
            let data_type = right.data_types[idx].clone();
            merged.add_column(column_id, qualified, unqualified, data_type);
        }
        
        Ok(merged)
    }
    
    /// Create Arrow Schema from ColumnSchema
    /// Each Field corresponds 1:1 with a ColumnId
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let fields: Vec<arrow::datatypes::Field> = self.column_ids
            .iter()
            .zip(self.qualified_names.iter())
            .zip(self.data_types.iter())
            .map(|((_, name), data_type)| {
                arrow::datatypes::Field::new(name, data_type.clone(), true)
            })
            .collect();
        
        arrow::datatypes::Schema::new(fields)
    }
}

impl Default for ColumnSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    
    #[test]
    fn test_add_column() {
        let mut schema = ColumnSchema::new();
        let col_id = ColumnId::new(1, 0);
        
        schema.add_column(
            col_id,
            "table.column".to_string(),
            "column".to_string(),
            DataType::Int64,
        );
        
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.physical_index(&col_id), Some(0));
    }
    
    #[test]
    fn test_resolve_qualified() {
        let mut schema = ColumnSchema::new();
        let col_id = ColumnId::new(1, 0);
        
        schema.add_column(
            col_id,
            "table.column".to_string(),
            "column".to_string(),
            DataType::Int64,
        );
        
        assert_eq!(schema.resolve_qualified("table.column"), Some(col_id));
        assert_eq!(schema.resolve_qualified("other.column"), None);
    }
    
    #[test]
    fn test_resolve_unqualified_ambiguous() {
        let mut schema = ColumnSchema::new();
        let col_id1 = ColumnId::new(1, 0);
        let col_id2 = ColumnId::new(2, 0);
        
        schema.add_column(
            col_id1,
            "table1.column".to_string(),
            "column".to_string(),
            DataType::Int64,
        );
        schema.add_column(
            col_id2,
            "table2.column".to_string(),
            "column".to_string(),
            DataType::Int64,
        );
        
        // Unqualified name should be ambiguous
        assert!(schema.resolve_unqualified("column").is_err());
        
        // Qualified names should work
        assert_eq!(schema.resolve_qualified("table1.column"), Some(col_id1));
        assert_eq!(schema.resolve_qualified("table2.column"), Some(col_id2));
    }
    
    #[test]
    fn test_merge() {
        let mut left = ColumnSchema::new();
        let mut right = ColumnSchema::new();
        
        left.add_column(
            ColumnId::new(1, 0),
            "left.col1".to_string(),
            "col1".to_string(),
            DataType::Int64,
        );
        
        right.add_column(
            ColumnId::new(2, 0),
            "right.col1".to_string(),
            "col1".to_string(),
            DataType::Int64,
        );
        
        let merged = ColumnSchema::merge(&left, &right).unwrap();
        assert_eq!(merged.len(), 2);
        
        // Both columns should be present
        assert!(merged.resolve_qualified("left.col1").is_some());
        assert!(merged.resolve_qualified("right.col1").is_some());
        
        // Unqualified name should be ambiguous
        assert!(merged.resolve_unqualified("col1").is_err());
    }
}

