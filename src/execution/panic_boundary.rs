/// Panic-hardened boundaries around root execution
/// Prevents server crashes by catching panics and converting them to errors
use crate::error::{EngineError, EngineResult};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Execute a function with panic boundary protection
/// Catches panics and converts them to EngineError::Internal
pub fn with_panic_boundary<F, T>(f: F) -> EngineResult<T>
where
    F: FnOnce() -> EngineResult<T>,
{
    catch_unwind(AssertUnwindSafe(f))
        .map_err(|panic_payload| {
            let message = if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "Unknown panic".to_string()
            };
            
            EngineError::internal(format!("Panic caught: {}", message))
        })?
}

/// Execute a function that returns Result<T, E> with panic boundary
/// Converts any error type to EngineError
pub fn with_panic_boundary_result<F, T, E>(f: F) -> EngineResult<T>
where
    F: FnOnce() -> Result<T, E>,
    E: std::fmt::Display,
{
    catch_unwind(AssertUnwindSafe(f))
        .map_err(|panic_payload| {
            let message = if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "Unknown panic".to_string()
            };
            
            EngineError::internal(format!("Panic caught: {}", message))
        })?
        .map_err(|e| EngineError::internal(format!("Execution error: {}", e)))
}

/// Macro to wrap operator execution with panic boundary
#[macro_export]
macro_rules! safe_execute {
    ($expr:expr) => {
        crate::execution::panic_boundary::with_panic_boundary(|| $expr)
    };
}

