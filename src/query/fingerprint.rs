/// Semantic query fingerprinting for plan reuse
/// Builds semantic fingerprints (NOT syntactic) for SQL queries to avoid re-planning
use crate::query::parser::ParsedQuery;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Semantic fingerprint of a query (structure-based, ignores literal constants)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueryFingerprint {
    /// Hash of query structure
    pub hash: u64,
    
    /// Human-readable fingerprint components (for debugging)
    pub components: FingerprintComponents,
}

/// Components of the fingerprint (for debugging and cache invalidation)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FingerprintComponents {
    /// Normalized table list (sorted, case-insensitive)
    pub tables: Vec<String>,
    
    /// Normalized join relationships (sorted)
    pub joins: Vec<JoinFingerprint>,
    
    /// Normalized filter structure (without literal values)
    pub filters: Vec<FilterFingerprint>,
    
    /// Normalized grouping keys
    pub group_by: Vec<String>,
    
    /// Window function signatures
    pub window_functions: Vec<String>,
    
    /// Aggregate function signatures
    pub aggregates: Vec<String>,
}

/// Join relationship fingerprint (structure only, no values)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinFingerprint {
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
    pub join_type: String,
}

/// Filter structure fingerprint (without literal values)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FilterFingerprint {
    pub table: String,
    pub column: String,
    pub operator: String, // "=", ">", "<", etc. (normalized)
    // Note: value is NOT included - only structure
}

/// Build semantic fingerprint from parsed query
pub fn fingerprint(parsed: &ParsedQuery) -> QueryFingerprint {
    // Normalize tables (sorted, lowercase)
    let mut tables: Vec<String> = parsed.tables.iter()
        .map(|t| t.to_lowercase())
        .collect();
    tables.sort();
    
    // Normalize joins
    let mut joins: Vec<JoinFingerprint> = parsed.joins.iter()
        .map(|j| JoinFingerprint {
            left_table: j.left_table.to_lowercase(),
            left_column: j.left_column.to_lowercase(),
            right_table: j.right_table.to_lowercase(),
            right_column: j.right_column.to_lowercase(),
            join_type: format!("{:?}", j.join_type).to_lowercase(),
        })
        .collect();
    joins.sort_by(|a, b| {
        a.left_table.cmp(&b.left_table)
            .then_with(|| a.left_column.cmp(&b.left_column))
            .then_with(|| a.right_table.cmp(&b.right_table))
            .then_with(|| a.right_column.cmp(&b.right_column))
    });
    
    // Normalize filters (structure only, no values)
    let mut filters: Vec<FilterFingerprint> = parsed.filters.iter()
        .map(|f| FilterFingerprint {
            table: f.table.to_lowercase(),
            column: f.column.to_lowercase(),
            operator: format!("{:?}", f.operator).to_lowercase(),
        })
        .collect();
    filters.sort_by(|a, b| {
        a.table.cmp(&b.table)
            .then_with(|| a.column.cmp(&b.column))
            .then_with(|| a.operator.cmp(&b.operator))
    });
    
    // Normalize group by
    let mut group_by: Vec<String> = parsed.group_by.iter()
        .map(|g| g.to_lowercase())
        .collect();
    group_by.sort();
    
    // Normalize window functions
    let mut window_functions: Vec<String> = parsed.window_functions.iter()
        .map(|wf| format!("{:?}", wf.function).to_lowercase())
        .collect();
    window_functions.sort();
    
    // Normalize aggregates
    let mut aggregates: Vec<String> = parsed.aggregates.iter()
        .map(|agg| format!("{:?}", agg.function).to_lowercase())
        .collect();
    aggregates.sort();
    
    let components = FingerprintComponents {
        tables,
        joins,
        filters,
        group_by,
        window_functions,
        aggregates,
    };
    
    // Compute hash
    let mut hasher = DefaultHasher::new();
    components.hash(&mut hasher);
    let hash = hasher.finish();
    
    QueryFingerprint {
        hash,
        components,
    }
}

/// Check if two queries have the same semantic structure
pub fn fingerprints_match(fp1: &QueryFingerprint, fp2: &QueryFingerprint) -> bool {
    fp1.hash == fp2.hash && fp1.components == fp2.components
}

