/// PipelineBuilder: Converts operator tree into execution pipelines
/// 
/// This module analyzes the operator tree and creates execution pipelines
/// that can run in parallel. Pipelines are sequences of operators that can
/// execute concurrently, with dependencies tracked for proper ordering.
use crate::execution::vectorized::{VectorOperator, VectorBatch};
use crate::error::EngineResult;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

/// Pipeline state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineState {
    /// Pipeline is ready to run
    Ready,
    /// Pipeline is currently running
    Running,
    /// Pipeline is blocked waiting for input
    Blocked,
    /// Pipeline has finished execution
    Finished,
    /// Pipeline encountered an error
    Error(String),
}

/// Execution pipeline - a sequence of operators that can execute together
#[derive(Clone)]
pub struct ExecutionPipeline {
    /// Pipeline ID
    pub id: usize,
    
    /// Operators in this pipeline (in execution order)
    pub operators: Vec<Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>>,
    
    /// Pipeline state
    pub state: PipelineState,
    
    /// Upstream pipeline IDs (pipelines that must complete before this one)
    pub upstream_pipelines: HashSet<usize>,
    
    /// Downstream pipeline IDs (pipelines that depend on this one)
    pub downstream_pipelines: HashSet<usize>,
    
    /// Current operator index being executed
    pub current_operator_idx: usize,
    
    /// Whether pipeline has been prepared
    pub prepared: bool,
}

impl ExecutionPipeline {
    /// Create a new execution pipeline
    pub fn new(id: usize, operators: Vec<Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>>) -> Self {
        Self {
            id,
            operators,
            state: PipelineState::Ready,
            upstream_pipelines: HashSet::new(),
            downstream_pipelines: HashSet::new(),
            current_operator_idx: 0,
            prepared: false,
        }
    }
    
    /// Add upstream dependency
    pub fn add_upstream(&mut self, pipeline_id: usize) {
        self.upstream_pipelines.insert(pipeline_id);
    }
    
    /// Add downstream dependency
    pub fn add_downstream(&mut self, pipeline_id: usize) {
        self.downstream_pipelines.insert(pipeline_id);
    }
    
    /// Check if pipeline is ready to run (all upstream pipelines finished)
    pub fn is_ready(&self, finished_pipelines: &HashSet<usize>) -> bool {
        self.upstream_pipelines.is_subset(finished_pipelines)
    }
    
    /// Get current operator
    pub fn current_operator(&self) -> Option<&Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>> {
        self.operators.get(self.current_operator_idx)
    }
    
    /// Advance to next operator
    pub fn advance(&mut self) -> bool {
        if self.current_operator_idx + 1 < self.operators.len() {
            self.current_operator_idx += 1;
            true
        } else {
            false
        }
    }
}

/// Pipeline builder - converts operator tree into pipelines
pub struct PipelineBuilder {
    /// Next pipeline ID
    next_pipeline_id: usize,
    
    /// Built pipelines
    pipelines: Vec<ExecutionPipeline>,
    
    /// Operator to pipeline mapping
    operator_to_pipeline: HashMap<usize, usize>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new() -> Self {
        Self {
            next_pipeline_id: 0,
            pipelines: Vec::new(),
            operator_to_pipeline: HashMap::new(),
        }
    }
    
    /// Build pipelines from operator tree
    /// 
    /// This analyzes the operator tree and creates execution pipelines.
    /// Operators that can run in parallel are grouped into separate pipelines.
    pub fn build_pipelines(
        &mut self,
        root_operator: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    ) -> EngineResult<Vec<ExecutionPipeline>> {
        info!("Building execution pipelines from operator tree");
        
        // For now, create a single pipeline with all operators
        // In a full implementation, we would analyze the tree to find
        // parallelizable sections (e.g., multiple scans, independent branches)
        let pipeline = ExecutionPipeline::new(
            self.next_pipeline_id,
            vec![root_operator],
        );
        
        self.pipelines.push(pipeline);
        self.next_pipeline_id += 1;
        
        debug!(
            num_pipelines = self.pipelines.len(),
            "Pipelines built"
        );
        
        Ok(self.pipelines.clone())
    }
    
    /// Build pipelines with dependency analysis
    /// 
    /// This version analyzes operator dependencies to create multiple pipelines
    /// that can execute in parallel where possible.
    pub fn build_pipelines_with_dependencies(
        &mut self,
        root_operator: Arc<tokio::sync::Mutex<Box<dyn VectorOperator>>>,
    ) -> EngineResult<Vec<ExecutionPipeline>> {
        info!("Building execution pipelines with dependency analysis");
        
        // Analyze operator tree to find independent branches
        // For joins, we can create separate pipelines for left and right sides
        // For now, we'll create a single pipeline but track dependencies
        
        let pipeline = ExecutionPipeline::new(
            self.next_pipeline_id,
            vec![root_operator],
        );
        
        self.pipelines.push(pipeline);
        self.next_pipeline_id += 1;
        
        debug!(
            num_pipelines = self.pipelines.len(),
            "Pipelines built with dependency analysis"
        );
        
        // Return a copy of the pipelines
        let mut result = Vec::new();
        for pipeline in &self.pipelines {
            // Create a new pipeline with cloned data
            let mut new_pipeline = ExecutionPipeline::new(
                pipeline.id,
                pipeline.operators.clone(),
            );
            new_pipeline.upstream_pipelines = pipeline.upstream_pipelines.clone();
            new_pipeline.downstream_pipelines = pipeline.downstream_pipelines.clone();
            result.push(new_pipeline);
        }
        Ok(result)
    }
    
    /// Get pipeline by ID
    pub fn get_pipeline(&self, pipeline_id: usize) -> Option<&ExecutionPipeline> {
        self.pipelines.get(pipeline_id)
    }
    
    /// Get mutable pipeline by ID
    pub fn get_pipeline_mut(&mut self, pipeline_id: usize) -> Option<&mut ExecutionPipeline> {
        self.pipelines.get_mut(pipeline_id)
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}
