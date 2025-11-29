/// Pipeline Model for DuckDB/Presto-grade join engine
/// 
/// This module defines the pipeline execution model with pipeline breakers
/// for efficient query execution.
use crate::query::plan::PlanOperator;
use anyhow::Result;

/// Pipeline operation types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineOp {
    /// Scan operation (reads from storage)
    Scan,
    /// Filter operation (applies predicates)
    Filter,
    /// Project operation (selects/transforms columns)
    Project,
    /// Hash build operation (builds hash table from right side of join)
    /// This is a pipeline breaker - must complete before probe can start
    HashBuild,
    /// Hash probe operation (probes hash table with left side of join)
    HashProbe,
    /// Sort operation (also a pipeline breaker)
    Sort,
    /// Aggregate operation
    Aggregate,
}

impl PipelineOp {
    /// Check if this operation is a pipeline breaker
    /// 
    /// Pipeline breakers must complete before the next pipeline can start.
    /// Examples: HashBuild (must build hash table before probing), Sort (must sort before output)
    pub fn is_pipeline_breaker(&self) -> bool {
        matches!(self, PipelineOp::HashBuild | PipelineOp::Sort)
    }
}

/// Pipeline - a sequence of operations that can execute in a streaming fashion
/// 
/// Pipelines flow data from one operation to the next without materialization
/// until a pipeline breaker is encountered.
#[derive(Debug, Clone)]
pub struct Pipeline {
    /// Operations in this pipeline (in execution order)
    pub operations: Vec<PipelineOp>,
    
    /// Whether this pipeline has a breaker (requires materialization)
    pub has_breaker: bool,
}

impl Pipeline {
    /// Create new empty pipeline
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
            has_breaker: false,
        }
    }
    
    /// Add an operation to the pipeline
    pub fn add_operation(&mut self, op: PipelineOp) {
        if op.is_pipeline_breaker() {
            self.has_breaker = true;
        }
        self.operations.push(op);
    }
    
    /// Check if pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
    
    /// Get number of operations
    pub fn len(&self) -> usize {
        self.operations.len()
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Pipeline Executor - coordinates pipeline execution
/// 
/// For hash joins, this manages:
/// 1. Build Pipelines: Build hash tables from right sides of joins
/// 2. Probe Pipeline: Single pipeline that flows through all probe operators
#[derive(Debug)]
pub struct PipelineExecutor {
    /// Build pipelines (one per join's right side)
    pub build_pipelines: Vec<Pipeline>,
    
    /// Probe pipeline (single pipeline for all probe operations)
    pub probe_pipeline: Pipeline,
}

impl PipelineExecutor {
    /// Create new pipeline executor
    pub fn new() -> Self {
        Self {
            build_pipelines: Vec::new(),
            probe_pipeline: Pipeline::new(),
        }
    }
    
    /// Add a build pipeline (for a join's right side)
    pub fn add_build_pipeline(&mut self, pipeline: Pipeline) {
        self.build_pipelines.push(pipeline);
    }
    
    /// Set the probe pipeline
    pub fn set_probe_pipeline(&mut self, pipeline: Pipeline) {
        self.probe_pipeline = pipeline;
    }
    
    /// Build pipelines from a query plan
    /// 
    /// This analyzes the plan and creates:
    /// - Build pipelines for each join's right side
    /// - A single probe pipeline for the entire query
    pub fn from_plan(plan: &PlanOperator) -> Result<Self> {
        let mut executor = Self::new();
        
        // Analyze plan to extract pipelines
        Self::extract_pipelines(plan, &mut executor)?;
        
        Ok(executor)
    }
    
    /// Extract pipelines from plan recursively
    fn extract_pipelines(plan: &PlanOperator, executor: &mut PipelineExecutor) -> Result<()> {
        match plan {
            PlanOperator::Join { left, right, .. } => {
                // Right side: build pipeline
                let mut build_pipeline = Pipeline::new();
                Self::build_pipeline_from_plan(right, &mut build_pipeline)?;
                build_pipeline.add_operation(PipelineOp::HashBuild);
                executor.add_build_pipeline(build_pipeline);
                
                // Left side: continue building probe pipeline
                Self::extract_pipelines(left, executor)?;
                
                // Add probe operation after left side is processed
                // (This will be added when we process the join itself)
            }
            PlanOperator::Scan { .. } => {
                // Scan is part of probe pipeline
                // (Will be added when building probe pipeline)
            }
            PlanOperator::Filter { input, .. } => {
                // Filter is part of probe pipeline
                Self::extract_pipelines(input, executor)?;
            }
            PlanOperator::Project { input, .. } => {
                // Project is part of probe pipeline
                Self::extract_pipelines(input, executor)?;
            }
            _ => {
                // For other operators, recurse on input
                // (Simplified - full implementation would handle all operators)
            }
        }
        
        Ok(())
    }
    
    /// Build a pipeline from a plan operator
    fn build_pipeline_from_plan(plan: &PlanOperator, pipeline: &mut Pipeline) -> Result<()> {
        match plan {
            PlanOperator::Scan { .. } => {
                pipeline.add_operation(PipelineOp::Scan);
            }
            PlanOperator::Filter { input, .. } => {
                Self::build_pipeline_from_plan(input, pipeline)?;
                pipeline.add_operation(PipelineOp::Filter);
            }
            PlanOperator::Project { input, .. } => {
                Self::build_pipeline_from_plan(input, pipeline)?;
                pipeline.add_operation(PipelineOp::Project);
            }
            PlanOperator::Join { left, right, .. } => {
                // For build pipeline, we only process the right side
                Self::build_pipeline_from_plan(right, pipeline)?;
            }
            _ => {
                // For other operators, recurse
            }
        }
        
        Ok(())
    }
}

impl Default for PipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pipeline_breaker() {
        assert!(PipelineOp::HashBuild.is_pipeline_breaker());
        assert!(PipelineOp::Sort.is_pipeline_breaker());
        assert!(!PipelineOp::Scan.is_pipeline_breaker());
        assert!(!PipelineOp::Filter.is_pipeline_breaker());
    }
    
    #[test]
    fn test_pipeline() {
        let mut pipeline = Pipeline::new();
        pipeline.add_operation(PipelineOp::Scan);
        pipeline.add_operation(PipelineOp::Filter);
        pipeline.add_operation(PipelineOp::HashBuild);
        
        assert_eq!(pipeline.len(), 3);
        assert!(pipeline.has_breaker);
    }
}

