/// Canonical Column Identity (ColId) Architecture
/// 
/// This module provides stable column identity throughout the operator pipeline,
/// solving schema resolution issues after JOINs, GROUP BY, and hypergraph reordering.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use arrow::datatypes::DataType;

/// Canonical Column Identifier
/// 
/// ColumnId is an immutable struct representing {table_id, column_index}.
/// It is assigned once at ScanOperator and preserved through all operators.
/// It never changes, even when:
/// - JOINs reorder columns
/// - Hypergraph optimizer changes join order
/// - Aliases are applied
/// 
/// Internal operations use ColumnId for access and computation.
/// External names (qualifiers, aliases) are only for display.
/// 
/// Design: ColumnId = {table_id, column_index}
/// - table_id: Unique identifier for the source table
/// - column_index: Index of column within that table's schema
/// 
/// This ensures stable, deterministic column identity throughout the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId {
    pub table_id: u32,
    pub column_index: u32,
}

impl ColumnId {
    /// Create a new ColumnId from table_id and column_index
    pub fn new(table_id: u32, column_index: u32) -> Self {
        ColumnId { table_id, column_index }
    }
    
    /// Get table_id
    pub fn table_id(&self) -> u32 {
        self.table_id
    }
    
    /// Get column_index
    pub fn column_index(&self) -> u32 {
        self.column_index
    }
    
    /// Convert to u32 for backward compatibility (if needed)
    /// Uses encoding: (table_id << 16) | column_index
    pub fn to_u32(&self) -> u32 {
        (self.table_id << 16) | self.column_index
    }
    
    /// Create from u32 (for backward compatibility)
    pub fn from_u32(value: u32) -> Self {
        ColumnId {
            table_id: value >> 16,
            column_index: value & 0xFFFF,
        }
    }
}

/// Legacy alias for backward compatibility during migration
/// TODO: Remove after full migration
pub type ColId = ColumnId;

/// External column name for display
pub type ExternalName = String;

/// Column field with identity
#[derive(Debug, Clone)]
pub struct ColumnField {
    pub column_id: ColumnId,
    pub external_name: ExternalName,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnField {
    pub fn new(column_id: ColumnId, external_name: ExternalName, data_type: DataType, nullable: bool) -> Self {
        ColumnField {
            column_id,
            external_name,
            data_type,
            nullable,
        }
    }
}

/// Column Schema with Canonical Identity
/// 
/// This structure carries both:
/// 1. Internal identity (ColumnIds) - stable, immutable, used for computation
/// 2. External names (name_map) - for SQL name resolution and display
/// 
/// Key invariants:
/// - column_ids array determines the physical order of columns
/// - Each Field in Arrow schema corresponds 1:1 with an entry in ColumnSchema
/// - Every column has both qualified and unqualified names stored
/// - ColumnIds are NEVER modified after creation (only computed/derived columns get new IDs)
/// - name_map provides lookup from external names to ColumnIds
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Ordered list of ColumnIds (determines physical column order)
    /// This maps 1:1 to Arrow schema fields
    pub column_ids: Vec<ColumnId>,
    
    /// Name map: external_name → ColumnId
    /// Supports:
    /// - Qualified names: "table.column" or "alias.column"
    /// - Unqualified names: "column" (only if unambiguous)
    /// - Aliases: "alias_name"
    pub name_map: HashMap<String, ColumnId>,
    
    /// Qualified names (one per ColumnId, in column_ids order)
    /// Format: "table.column" or "alias.column"
    pub qualified_names: Vec<ExternalName>,
    
    /// Unqualified names (one per ColumnId, in column_ids order)
    /// Format: "column"
    pub unqualified_names: Vec<ExternalName>,
    
    /// Data types (one per ColumnId, in column_ids order)
    pub data_types: Vec<DataType>,
}

impl ColumnSchema {
    /// Create new schema
    pub fn new() -> Self {
        Self {
            column_ids: Vec::new(),
            name_map: HashMap::new(),
            qualified_names: Vec::new(),
            unqualified_names: Vec::new(),
            data_types: Vec::new(),
        }
    }
    
    /// Add a column with both qualified and unqualified names
    /// 
    /// # Arguments
    /// * `column_id` - Immutable ColumnId (table_id, column_index)
    /// * `qualified_name` - Fully qualified name: "table.column" or "alias.column"
    /// * `unqualified_name` - Unqualified name: "column"
    /// * `data_type` - Arrow data type
    /// 
    /// # Rules
    /// - ColumnId is NEVER modified after creation
    /// - Unqualified name is only added to name_map if unambiguous
    /// - Qualified name always takes precedence
    pub fn add_column(
        &mut self,
        column_id: ColumnId,
        qualified_name: ExternalName,
        unqualified_name: ExternalName,
        data_type: DataType,
    ) {
        self.column_ids.push(column_id);
        self.qualified_names.push(qualified_name.clone());
        self.unqualified_names.push(unqualified_name.clone());
        self.data_types.push(data_type);
        
        // Always add qualified name to name_map
        self.name_map.insert(qualified_name.clone(), column_id);
        
        // Add unqualified name only if it doesn't conflict
        // If conflict exists, qualified name takes precedence (error will be raised on resolution)
        if !self.name_map.contains_key(&unqualified_name) {
            self.name_map.insert(unqualified_name.clone(), column_id);
        }
    }
    
    /// Add a column with automatic name parsing
    /// Extracts qualified and unqualified names from a single string
    pub fn add_column_from_name(
        &mut self,
        column_id: ColumnId,
        name: ExternalName,
        data_type: DataType,
    ) {
        let (qualified, unqualified) = if name.contains('.') {
            let parts: Vec<&str> = name.split('.').collect();
            let unqualified = parts.last().map(|s| s.to_string()).unwrap_or_else(|| name.clone());
            (name.clone(), unqualified)
        } else {
            // Unqualified name - qualified will be set later or remain unqualified
            (name.clone(), name.clone())
        };
        
        self.add_column(column_id, qualified, unqualified, data_type);
    }
    
    /// Resolve external name to ColumnId
    /// Returns None if name not found
    /// 
    /// # Resolution Rules
    /// 1. Exact match (case-sensitive) in name_map
    /// 2. If ambiguous (multiple matches), returns None (caller must error)
    pub fn resolve(&self, name: &str) -> Option<ColumnId> {
        self.name_map.get(name).copied()
    }
    
    /// Resolve qualified name (e.g., "table.column" or "alias.column")
    /// This is the preferred method for resolving qualified column references.
    /// 
    /// # Returns
    /// - Some(ColumnId) if exactly one match found
    /// - None if not found or ambiguous
    pub fn resolve_qualified(&self, qualified: &str) -> Option<ColumnId> {
        // First try exact match
        if let Some(&col_id) = self.name_map.get(qualified) {
            return Some(col_id);
        }
        
        // If not found, try matching by qualified name pattern
        // This handles cases where qualified name might be stored differently
        for (idx, qualified_name) in self.qualified_names.iter().enumerate() {
            if qualified_name == qualified {
                return Some(self.column_ids[idx]);
            }
        }
        
        None
    }
    
    /// Resolve unqualified name with ambiguity check
    /// Returns error if ambiguous (multiple columns with same unqualified name)
    pub fn resolve_unqualified(&self, name: &str) -> Result<ColumnId, String> {
        // Check if name is already qualified
        if name.contains('.') {
            return self.resolve(name)
                .ok_or_else(|| format!("Column '{}' not found", name));
        }
        
        // Find all columns with this unqualified name
        let matches: Vec<ColumnId> = self.unqualified_names
            .iter()
            .enumerate()
            .filter(|(_, unqualified)| unqualified == &name)
            .map(|(idx, _)| self.column_ids[idx])
            .collect();
        
        match matches.len() {
            0 => Err(format!("Column '{}' not found", name)),
            1 => Ok(matches[0]),
            _ => Err(format!(
                "Ambiguous column reference '{}': found {} matches. Use qualified name.",
                name, matches.len()
            )),
        }
    }
    
    /// Get qualified name for ColumnId
    pub fn qualified_name(&self, column_id: ColumnId) -> Option<&ExternalName> {
        self.column_ids
            .iter()
            .position(|&id| id == column_id)
            .and_then(|idx| self.qualified_names.get(idx))
    }
    
    /// Get unqualified name for ColumnId
    pub fn unqualified_name(&self, column_id: ColumnId) -> Option<&ExternalName> {
        self.column_ids
            .iter()
            .position(|&id| id == column_id)
            .and_then(|idx| self.unqualified_names.get(idx))
    }
    
    /// Get data type for ColumnId
    pub fn data_type(&self, column_id: ColumnId) -> Option<&DataType> {
        self.column_ids
            .iter()
            .position(|&id| id == column_id)
            .and_then(|idx| self.data_types.get(idx))
    }
    
    /// Get physical index for ColumnId (for array access)
    /// This is the index in the Arrow array/ColumnarBatch
    pub fn physical_index(&self, column_id: ColumnId) -> Option<usize> {
        self.column_ids.iter().position(|&id| id == column_id)
    }
    
    /// Get ColumnId at physical index
    pub fn column_id_at(&self, index: usize) -> Option<ColumnId> {
        self.column_ids.get(index).copied()
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
            
            // Check for unqualified name conflicts
            if merged.name_map.contains_key(&unqualified) {
                // Ambiguous - but we still add it (error will be raised on resolution)
                // This allows the merge to complete, but resolution will fail if ambiguous
            }
            
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
    
    /// Get number of columns
    pub fn len(&self) -> usize {
        self.column_ids.len()
    }
    
    /// Check if schema is empty
    pub fn is_empty(&self) -> bool {
        self.column_ids.is_empty()
    }
}

/// Generate ColumnId based on table and column index
/// This ensures stable, deterministic ColumnIds for the same table/column combination
/// 
/// # Arguments
/// * `table_id` - Unique identifier for the source table
/// * `column_index` - Index of column within that table's schema
/// 
/// # Returns
/// ColumnId with table_id and column_index
pub fn generate_column_id_for_table_column(table_id: u32, column_index: u32) -> ColumnId {
    ColumnId::new(table_id, column_index)
}

/// Generate a new unique ColumnId for computed/derived columns
/// Only use this for columns that don't come from base tables (e.g., expressions, aggregations)
/// 
/// # Warning
/// For base table columns, always use generate_column_id_for_table_column() for stability
static COL_ID_GENERATOR: AtomicU32 = AtomicU32::new(1);

pub fn generate_column_id_for_computed() -> ColumnId {
    let id = COL_ID_GENERATOR.fetch_add(1, Ordering::Relaxed);
    // Use high table_id to distinguish from base table columns
    ColumnId::new(0xFFFF, id)
}

/// Legacy functions for backward compatibility during migration
pub fn generate_col_id() -> ColId {
    generate_column_id_for_computed()
}

pub fn generate_col_id_for_table_column(table_id: u32, column_index: u32) -> ColId {
    generate_column_id_for_table_column(table_id, column_index)
}


