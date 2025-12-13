/// Unified error type for the SQL engine
/// Provides structured error handling with categories for different failure modes
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum EngineError {
    /// Execution errors: operator failures, data corruption, invalid state
    #[error("Execution error: {message}")]
    Execution {
        message: String,
        operator: Option<String>,
        context: Option<String>,
    },
    
    /// Planning errors: query planning, optimization failures
    #[error("Planning error: {message}")]
    Planning {
        message: String,
        query: Option<String>,
        context: Option<String>,
    },
    
    /// Memory errors: OOM, allocation failures, spill failures
    #[error("Memory error: {message}")]
    Memory {
        message: String,
        limit: Option<usize>,
        used: Option<usize>,
        context: Option<String>,
    },
    
    /// IO errors: file operations, network, spill file I/O
    #[error("IO error: {message}")]
    IO {
        message: String,
        path: Option<String>,
        source_message: Option<String>,
    },
    
    /// Cancellation errors: query timeout, user cancellation
    #[error("Cancellation: {message}")]
    Cancellation {
        message: String,
        reason: Option<String>,
    },
    
    /// Internal errors: should never happen, indicates bug
    #[error("Internal error: {message}")]
    Internal {
        message: String,
        context: Option<String>,
    },
}

impl EngineError {
    pub fn execution(message: impl Into<String>) -> Self {
        Self::Execution {
            message: message.into(),
            operator: None,
            context: None,
        }
    }
    
    pub fn execution_with_operator(message: impl Into<String>, operator: impl Into<String>) -> Self {
        Self::Execution {
            message: message.into(),
            operator: Some(operator.into()),
            context: None,
        }
    }
    
    pub fn planning(message: impl Into<String>) -> Self {
        Self::Planning {
            message: message.into(),
            query: None,
            context: None,
        }
    }
    
    pub fn memory(message: impl Into<String>) -> Self {
        Self::Memory {
            message: message.into(),
            limit: None,
            used: None,
            context: None,
        }
    }
    
    pub fn memory_with_usage(message: impl Into<String>, limit: usize, used: usize) -> Self {
        Self::Memory {
            message: message.into(),
            limit: Some(limit),
            used: Some(used),
            context: None,
        }
    }
    
    pub fn io(message: impl Into<String>) -> Self {
        Self::IO {
            message: message.into(),
            path: None,
            source_message: None,
        }
    }
    
    pub fn io_with_path(message: impl Into<String>, path: impl Into<String>) -> Self {
        Self::IO {
            message: message.into(),
            path: Some(path.into()),
            source_message: None,
        }
    }
    
    pub fn cancellation(message: impl Into<String>) -> Self {
        Self::Cancellation {
            message: message.into(),
            reason: None,
        }
    }
    
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            context: None,
        }
    }
    
    /// Add context to an error
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        match &mut self {
            Self::Execution { context: ctx, .. } => *ctx = Some(context.into()),
            Self::Planning { context: ctx, .. } => *ctx = Some(context.into()),
            Self::Memory { context: ctx, .. } => *ctx = Some(context.into()),
            Self::Internal { context: ctx, .. } => *ctx = Some(context.into()),
            _ => {}
        }
        self
    }
}

impl From<anyhow::Error> for EngineError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
            context: None,
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        Self::IO {
            message: err.to_string(),
            path: None,
            source_message: None,
        }
    }
}

/// Result type alias for engine operations
pub type EngineResult<T> = Result<T, EngineError>;

