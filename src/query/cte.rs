/// Common Table Expression (CTE) Support
/// Handles WITH clauses and recursive CTEs
use sqlparser::ast::*;
use crate::query::plan::*;
use crate::hypergraph::graph::HyperGraph;
use anyhow::Result;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};

/// CTE context - stores CTE definitions
#[derive(Clone, Debug)]
pub struct CTEContext {
    /// Map from CTE name to its query plan
    ctes: HashMap<String, CTEDefinition>,
}

#[derive(Clone, Debug)]
pub struct CTEDefinition {
    /// CTE name
    pub name: String,
    /// Column names (if specified)
    pub columns: Vec<String>,
    /// Query that defines the CTE
    pub query: Box<Query>,
    /// Whether this is a recursive CTE
    pub recursive: bool,
}

impl CTEContext {
    pub fn new() -> Self {
        Self {
            ctes: HashMap::new(),
        }
    }
    
    /// Extract CTEs from query
    pub fn from_query(query: &Query) -> Result<Self> {
        let mut ctes = HashMap::new();
        
        // Check if query has WITH clause
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();
                let columns = cte.alias.columns.iter()
                    .map(|c| c.value.clone())
                    .collect();
                
                let cte_def = CTEDefinition {
                    name: name.clone(),
                    columns,
                    query: cte.query.clone(),
                    recursive: with.recursive,
                };
                
                ctes.insert(name, cte_def);
            }
        }
        
        Ok(Self { ctes })
    }
    
    /// Get CTE definition by name
    pub fn get(&self, name: &str) -> Option<&CTEDefinition> {
        self.ctes.get(name)
    }
    
    /// Check if CTE exists
    pub fn contains(&self, name: &str) -> bool {
        self.ctes.contains_key(name)
    }
    
    /// Get all CTE names
    pub fn names(&self) -> Vec<String> {
        self.ctes.keys().cloned().collect()
    }
    
    /// Detect mutual CTE dependencies (cycles in CTE dependency graph)
    /// Returns error if cycle is detected, which usually indicates a query design error
    pub fn detect_mutual_dependencies(&self) -> Result<()> {
        use std::collections::{HashSet, VecDeque};
        
        // Build dependency graph: CTE name -> set of CTEs it references
        let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();
        
        for (cte_name, cte_def) in &self.ctes {
            let mut deps = HashSet::new();
            
            // Extract table references from CTE query
            self.extract_cte_references(&cte_def.query, &mut deps);
            
            // Remove self-reference (allowed for recursive CTEs)
            deps.remove(cte_name);
            
            dependencies.insert(cte_name.clone(), deps);
        }
        
        // Detect cycles using DFS
        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();
        
        for cte_name in self.ctes.keys() {
            if !visited.contains(cte_name) {
                if self.has_cycle(cte_name, &dependencies, &mut visited, &mut recursion_stack) {
                    anyhow::bail!(
                        "Mutual CTE dependency detected (cycle in CTE dependency graph). \
                        This usually indicates a query design error. Consider restructuring your CTEs."
                    );
                }
            }
        }
        
        Ok(())
    }
    
    /// Extract CTE references from a query AST
    /// This is a simplified implementation that uses string matching
    /// A full implementation would properly walk the AST
    fn extract_cte_references(&self, query: &Query, deps: &mut HashSet<String>) {
        // Simple extraction: look for table references that match CTE names
        // This is a simplified version - full implementation would walk the AST
        let query_str = format!("{:?}", query);
        
        for cte_name in self.ctes.keys() {
            // Check if query string contains this CTE name as a table reference
            // This is a heuristic - in production, we'd properly walk the AST
            // We look for the CTE name as a standalone word to avoid false positives
            let pattern = format!(" {} ", cte_name); // Space before and after
            if query_str.contains(&pattern) || 
               query_str.starts_with(&format!("{}", cte_name)) ||
               query_str.ends_with(&format!("{}", cte_name)) {
                deps.insert(cte_name.clone());
            }
        }
    }
    
    /// Check for cycles in dependency graph using DFS
    fn has_cycle(
        &self,
        node: &str,
        dependencies: &HashMap<String, HashSet<String>>,
        visited: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(node.to_string());
        recursion_stack.insert(node.to_string());
        
        if let Some(deps) = dependencies.get(node) {
            for dep in deps {
                if !visited.contains(dep) {
                    if self.has_cycle(dep, dependencies, visited, recursion_stack) {
                        return true;
                    }
                } else if recursion_stack.contains(dep) {
                    // Found a back edge - cycle detected
                    return true;
                }
            }
        }
        
        recursion_stack.remove(node);
        false
    }
    
    /// Check if a CTE is recursive (references itself)
    pub fn is_recursive(&self, cte_name: &str) -> bool {
        if let Some(cte_def) = self.ctes.get(cte_name) {
            // Check if CTE is marked as recursive (WITH RECURSIVE)
            if cte_def.recursive {
                return true;
            }
            
            // Also check if it actually references itself
            let query_str = format!("{:?}", cte_def.query);
            query_str.contains(cte_name)
        } else {
            false
        }
    }
}

impl Default for CTEContext {
    fn default() -> Self {
        Self::new()
    }
}

/// CTE resolver - resolves CTE references in queries
pub struct CTEResolver {
    /// CTE context
    context: CTEContext,
    /// Hypergraph for planning CTE queries
    graph: Arc<HyperGraph>,
}

impl CTEResolver {
    pub fn new(context: CTEContext, graph: Arc<HyperGraph>) -> Self {
        Self { context, graph }
    }
    
    /// Resolve CTE reference to a query plan
    pub fn resolve_cte(&self, cte_name: &str) -> Result<QueryPlan> {
        let cte_def = self.context.get(cte_name)
            .ok_or_else(|| anyhow::anyhow!("CTE '{}' not found", cte_name))?;
        
        // Parse and plan the CTE query
        // TODO: This requires integrating with the query planner
        // For now, return a placeholder plan
        anyhow::bail!("CTE resolution not yet fully implemented")
    }
    
    /// Check if a table reference is actually a CTE
    pub fn is_cte(&self, name: &str) -> bool {
        self.context.contains(name)
    }
}

