/// Unified Column Resolution
/// Provides canonical column resolution logic for all operators
use arrow::datatypes::{Schema, SchemaRef};
use std::collections::HashMap;
use anyhow::{Result, bail};

/// ColumnResolver - Unified column resolution logic
/// Resolves column names (qualified, unqualified, alias) using schema and table aliases
pub struct ColumnResolver {
    schema: SchemaRef,
    table_aliases: HashMap<String, String>, // alias -> actual table name
}

impl ColumnResolver {
    /// Create a new ColumnResolver
    pub fn new(schema: SchemaRef, table_aliases: HashMap<String, String>) -> Self {
        Self {
            schema,
            table_aliases,
        }
    }

    /// Create a ColumnResolver with empty table aliases
    pub fn from_schema(schema: SchemaRef) -> Self {
        Self {
            schema,
            table_aliases: HashMap::new(),
        }
    }

    /// Resolve a column name to a column index
    /// Supports:
    /// - Qualified names: "table.column" or "alias.column"
    /// - Unqualified names: "column" (with disambiguation if needed)
    /// - Case-insensitive matching
    /// - Table alias resolution
    pub fn resolve(&self, column_name: &str) -> Result<usize> {
        // Normalize column name (trim whitespace)
        let column_name = column_name.trim();
        
        // Strategy 0: Exact match by iterating fields (most reliable for qualified names)
        // This handles cases where Arrow's index_of might fail due to case sensitivity or other issues
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let field_name = field.name().trim();
            // Exact match (case-sensitive first for speed)
            if field_name == column_name {
                return Ok(idx);
            }
            // Case-insensitive exact match
            if field_name.eq_ignore_ascii_case(column_name) {
                return Ok(idx);
            }
        }
        
        // Strategy 1: Direct qualified name lookup (e.g., "e1.name" or "employees.name")
        if column_name.contains('.') {
            if let Ok(idx) = self.schema.index_of(column_name) {
                return Ok(idx);
            }

            // Try case-insensitive qualified lookup
            for (idx, field) in self.schema.fields().iter().enumerate() {
                if field.name().trim().eq_ignore_ascii_case(column_name) {
                    return Ok(idx);
                }
            }

            // Try to resolve table alias to actual table name
            let parts: Vec<&str> = column_name.split('.').collect();
            if parts.len() == 2 {
                let table_part = parts[0].trim();
                let col_part = parts[1].trim();

                // If table_part is an alias, resolve it to actual table name
                if let Some(actual_table) = self.table_aliases.get(table_part) {
                    let table_qualified = format!("{}.{}", actual_table, col_part);
                    if let Ok(idx) = self.schema.index_of(&table_qualified) {
                        return Ok(idx);
                    }
                    // Try case-insensitive
                    for (idx, field) in self.schema.fields().iter().enumerate() {
                        if field.name().trim().eq_ignore_ascii_case(&table_qualified) {
                            return Ok(idx);
                        }
                    }
                }

                // Try with table alias directly (e.g., "e1.name" -> schema might have "e1.name")
                // This handles cases where schema already has alias-prefixed columns
                for (idx, field) in self.schema.fields().iter().enumerate() {
                    let field_name = field.name().trim();
                    if field_name == column_name || field_name.eq_ignore_ascii_case(column_name) {
                        return Ok(idx);
                    }
                }

                // Fall back to unqualified column name
                return self.resolve(col_part);
            }
        }

        // Strategy 2: Direct unqualified name lookup
        if let Ok(idx) = self.schema.index_of(column_name) {
            return Ok(idx);
        }

        // Strategy 3: Case-insensitive matching
        let upper_name = column_name.to_uppercase();
        if let Ok(idx) = self.schema.index_of(&upper_name) {
            return Ok(idx);
        }
        let lower_name = column_name.to_lowercase();
        if let Ok(idx) = self.schema.index_of(&lower_name) {
            return Ok(idx);
        }

        // Strategy 4: Try to find column in qualified fields (e.g., find "name" in "e1.name")
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let field_name = field.name().trim();
            
            // Exact match (case-insensitive)
            if field_name.eq_ignore_ascii_case(column_name) {
                return Ok(idx);
            }

            // Match unqualified part of qualified field (e.g., "name" matches "e1.name")
            if field_name.contains('.') {
                let parts: Vec<&str> = field_name.split('.').collect();
                if parts.len() == 2 {
                    let col_part = parts[1].trim();
                    if col_part.eq_ignore_ascii_case(column_name) {
                        // Check if there's ambiguity (multiple tables have this column)
                        // For now, return first match
                        return Ok(idx);
                    }
                }
            }

            // Fuzzy match: normalize by removing special chars
            let norm_field = field_name.to_uppercase().replace(&['(', ')', '*', ' '][..], "");
            let norm_col = column_name.to_uppercase().replace(&['(', ')', '*', ' '][..], "");
            if norm_field == norm_col {
                return Ok(idx);
            }
        }

        // Strategy 5: Try all possible qualified combinations with table aliases
        for (alias, _actual_table) in &self.table_aliases {
            let qualified = format!("{}.{}", alias, column_name);
            if let Ok(idx) = self.schema.index_of(&qualified) {
                return Ok(idx);
            }
            // Try case-insensitive
            for (idx, field) in self.schema.fields().iter().enumerate() {
                if field.name().trim().eq_ignore_ascii_case(&qualified) {
                    return Ok(idx);
                }
            }
        }

        // All strategies failed
        let available_cols: Vec<String> = self.schema.fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        bail!(
            "Column '{}' not found. Available columns: {:?}",
            column_name,
            available_cols
        );
    }

    /// Resolve a column name, returning None if not found instead of error
    pub fn try_resolve(&self, column_name: &str) -> Option<usize> {
        self.resolve(column_name).ok()
    }

    /// Get available column names in the schema
    pub fn available_columns(&self) -> Vec<String> {
        self.schema.fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, DataType};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        let fields = vec![
            Arc::new(Field::new("e1.name", DataType::Utf8, true)),
            Arc::new(Field::new("e1.salary", DataType::Int64, true)),
            Arc::new(Field::new("e2.name", DataType::Utf8, true)),
            Arc::new(Field::new("e2.salary", DataType::Int64, true)),
        ];
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_qualified_resolution() {
        let schema = create_test_schema();
        let resolver = ColumnResolver::from_schema(schema);
        
        assert_eq!(resolver.resolve("e1.name").unwrap(), 0);
        assert_eq!(resolver.resolve("e1.salary").unwrap(), 1);
        assert_eq!(resolver.resolve("e2.name").unwrap(), 2);
    }

    #[test]
    fn test_unqualified_resolution() {
        let schema = create_test_schema();
        let resolver = ColumnResolver::from_schema(schema);
        
        // Should match first occurrence
        assert_eq!(resolver.resolve("name").unwrap(), 0);
    }
}

