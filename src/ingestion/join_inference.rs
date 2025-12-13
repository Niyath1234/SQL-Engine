//! Join Inference Engine - Proposes joins with confidence (does NOT authorize)

use crate::worldstate::schema::SchemaRegistry;
use crate::worldstate::keys::KeyRegistry;
use crate::worldstate::rules::{JoinRule, RuleState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Inferred join proposal
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinProposal {
    /// Proposed rule
    pub rule: JoinRule,
    
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
    
    /// Evidence for this proposal
    pub evidence: Vec<String>,
}

/// Join Inference Engine
pub struct JoinInference {
    /// Minimum confidence to propose (default: 0.5)
    min_confidence: f64,
}

impl JoinInference {
    pub fn new() -> Self {
        Self {
            min_confidence: 0.5,
        }
    }
    
    /// Infer join rules from schema and keys
    /// 
    /// This proposes joins but does NOT approve them.
    /// Approval must happen through the rule registry.
    pub fn infer_joins(
        &self,
        schema_registry: &SchemaRegistry,
        key_registry: &KeyRegistry,
    ) -> Vec<JoinProposal> {
        let mut proposals = Vec::new();
        let tables: Vec<String> = schema_registry.list_tables();
        
        // Compare all pairs of tables
        for i in 0..tables.len() {
            for j in (i + 1)..tables.len() {
                let left_table = &tables[i];
                let right_table = &tables[j];
                
                // Try to find join opportunities
                if let Some(proposal) = self.infer_join_between_tables(
                    schema_registry,
                    key_registry,
                    left_table,
                    right_table,
                ) {
                    if proposal.confidence >= self.min_confidence {
                        proposals.push(proposal);
                    }
                }
            }
        }
        
        proposals
    }
    
    /// Infer join between two specific tables
    fn infer_join_between_tables(
        &self,
        schema_registry: &SchemaRegistry,
        key_registry: &KeyRegistry,
        left_table: &str,
        right_table: &str,
    ) -> Option<JoinProposal> {
        let left_schema = schema_registry.get_table(left_table)?;
        let right_schema = schema_registry.get_table(right_table)?;
        
        // Strategy 1: Foreign key pattern (left_table_id in right table)
        if let Some((left_key, right_key, confidence)) = self.check_foreign_key_pattern(
            left_table,
            &left_schema.columns,
            right_table,
            &right_schema.columns,
        ) {
            let mut rule = JoinRule::new(
                format!("inferred_{}_{}", left_table, right_table),
                left_table.to_string(),
                vec![left_key.clone()],
                right_table.to_string(),
                vec![right_key.clone()],
                "inner".to_string(),
                "1:N".to_string(), // Foreign key typically means 1:N
            );
            rule.confidence = Some(confidence);
            rule.justification = Some(format!(
                "Inferred: {} column in {} matches {} primary key",
                right_key, right_table, left_table
            ));
            
            return Some(JoinProposal {
                rule,
                confidence,
                evidence: vec![
                    format!("Foreign key pattern detected: {}.{} -> {}.{}", 
                            right_table, right_key, left_table, left_key),
                ],
            });
        }
        
        // Strategy 2: Reverse foreign key (right_table_id in left table)
        if let Some((left_key, right_key, confidence)) = self.check_foreign_key_pattern(
            right_table,
            &right_schema.columns,
            left_table,
            &left_schema.columns,
        ) {
            let mut rule = JoinRule::new(
                format!("inferred_{}_{}", right_table, left_table),
                right_table.to_string(),
                vec![right_key.clone()],
                left_table.to_string(),
                vec![left_key.clone()],
                "inner".to_string(),
                "1:N".to_string(),
            );
            rule.confidence = Some(confidence);
            rule.justification = Some(format!(
                "Inferred: {} column in {} matches {} primary key ({})",
                left_key, left_table, right_table, right_key
            ));
            
            return Some(JoinProposal {
                rule,
                confidence,
                evidence: vec![
                    format!("Foreign key pattern detected: {}.{} -> {}.{}", 
                            left_table, left_key, right_table, right_key),
                ],
            });
        }
        
        // Strategy 3: Same column name (lower confidence)
        if let Some((col_name, confidence)) = self.check_same_column_name(
            &left_schema.columns,
            &right_schema.columns,
        ) {
            let col_name_clone = col_name.clone();
            let mut rule = JoinRule::new(
                format!("inferred_{}_{}_samecol", left_table, right_table),
                left_table.to_string(),
                vec![col_name.clone()],
                right_table.to_string(),
                vec![col_name],
                "inner".to_string(),
                "N:M".to_string(), // Unknown cardinality
            );
            rule.confidence = Some(confidence);
            rule.justification = Some(format!(
                "Inferred: Both tables have column '{}'",
                col_name_clone
            ));
            
            return Some(JoinProposal {
                rule,
                confidence,
                evidence: vec![
                    "Same column name in both tables".to_string(),
                ],
            });
        }
        
        None
    }
    
    /// Check for foreign key pattern: {parent_table}_id in child table
    fn check_foreign_key_pattern(
        &self,
        parent_table: &str,
        parent_columns: &[crate::worldstate::schema::ColumnInfo],
        child_table: &str,
        child_columns: &[crate::worldstate::schema::ColumnInfo],
    ) -> Option<(String, String, f64)> {
        // Look for primary key in parent
        let parent_pk = parent_columns.iter()
            .find(|c| {
                let name_lower = c.name.to_lowercase();
                name_lower == "id" || name_lower == format!("{}_id", parent_table.to_lowercase())
            })?;
        
        // Look for matching foreign key in child
        let fk_pattern = format!("{}_id", parent_table.to_lowercase());
        let child_fk = child_columns.iter()
            .find(|c| c.name.to_lowercase() == fk_pattern)?;
        
        // Check type compatibility
        if parent_pk.data_type == child_fk.data_type {
            // High confidence for foreign key pattern
            Some((
                parent_pk.name.clone(),
                child_fk.name.clone(),
                0.8, // High confidence
            ))
        } else {
            None
        }
    }
    
    /// Check for same column name in both tables
    fn check_same_column_name(
        &self,
        left_columns: &[crate::worldstate::schema::ColumnInfo],
        right_columns: &[crate::worldstate::schema::ColumnInfo],
    ) -> Option<(String, f64)> {
        for left_col in left_columns {
            for right_col in right_columns {
                if left_col.name == right_col.name && left_col.data_type == right_col.data_type {
                    // Lower confidence for same name (could be coincidence)
                    return Some((left_col.name.clone(), 0.3));
                }
            }
        }
        None
    }
}

impl Default for JoinInference {
    fn default() -> Self {
        Self::new()
    }
}

