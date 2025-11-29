/// Operator Contracts and Termination Guarantees
/// 
/// This module defines and enforces strict contracts for all operators
/// to prevent infinite loops and ensure proper termination.
use anyhow::Result;

/// Core operator contract: next() must eventually return None
/// 
/// # Contract
/// - next() must eventually return Ok(None) after a finite number of Ok(Some(batch)) calls
/// - next() must not return Ok(Some(batch)) with row_count == 0 in an infinite loop
/// - Operators must transition to a terminal state (Finished, Exhausted, etc.)
/// 
/// # Safety Counters
/// - MAX_BATCHES: Maximum number of batches an operator can produce (1M)
/// - MAX_EMPTY_BATCHES: Maximum consecutive empty batches (100)
/// - MAX_EXECUTION_TIME: Maximum execution time in seconds (60)
pub struct OperatorContracts {
    /// Total batches produced
    pub total_batches: usize,
    
    /// Consecutive empty batches
    pub consecutive_empty_batches: usize,
    
    /// Maximum batches allowed
    pub max_batches: usize,
    
    /// Maximum consecutive empty batches allowed
    pub max_empty_batches: usize,
}

impl OperatorContracts {
    /// Create new operator contracts with default limits
    pub fn new() -> Self {
        Self {
            total_batches: 0,
            consecutive_empty_batches: 0,
            max_batches: 1_000_000, // 1M batches
            max_empty_batches: 100, // 100 consecutive empty batches
        }
    }
    
    /// Create with custom limits
    pub fn with_limits(max_batches: usize, max_empty_batches: usize) -> Self {
        Self {
            total_batches: 0,
            consecutive_empty_batches: 0,
            max_batches,
            max_empty_batches,
        }
    }
    
    /// Record a batch production
    /// 
    /// # Returns
    /// - Ok(()) if within limits
    /// - Err if limits exceeded
    pub fn record_batch(&mut self, row_count: usize) -> Result<()> {
        self.total_batches += 1;
        
        // Check total batches limit
        if self.total_batches > self.max_batches {
            return Err(anyhow::anyhow!(
                "ERR_OPERATOR_CONTRACT_VIOLATION: Operator exceeded MAX_BATCHES limit ({}). \
                This indicates a possible infinite loop. Operator must eventually return None.",
                self.max_batches
            ));
        }
        
        // Check empty batch limit
        if row_count == 0 {
            self.consecutive_empty_batches += 1;
            if self.consecutive_empty_batches > self.max_empty_batches {
                return Err(anyhow::anyhow!(
                    "ERR_OPERATOR_CONTRACT_VIOLATION: Operator produced {} consecutive empty batches. \
                    This indicates a possible infinite loop. Operator must eventually produce rows or return None.",
                    self.max_empty_batches
                ));
            }
        } else {
            // Reset empty batch counter
            self.consecutive_empty_batches = 0;
        }
        
        Ok(())
    }
    
    /// Reset counters (for operator reuse)
    pub fn reset(&mut self) {
        self.total_batches = 0;
        self.consecutive_empty_batches = 0;
    }
}

impl Default for OperatorContracts {
    fn default() -> Self {
        Self::new()
    }
}

/// Execution engine timeout guard
/// 
/// Ensures queries don't run indefinitely
pub struct ExecutionTimeout {
    start_time: std::time::Instant,
    max_duration_secs: u64,
}

impl ExecutionTimeout {
    /// Create new timeout guard
    pub fn new(max_duration_secs: u64) -> Self {
        Self {
            start_time: std::time::Instant::now(),
            max_duration_secs,
        }
    }
    
    /// Check if timeout exceeded
    /// 
    /// # Returns
    /// - Ok(()) if within timeout
    /// - Err if timeout exceeded
    pub fn check(&self) -> Result<()> {
        let elapsed = self.start_time.elapsed().as_secs();
        if elapsed > self.max_duration_secs {
            return Err(anyhow::anyhow!(
                "ERR_EXECUTION_TIMEOUT: Query execution exceeded {} seconds. \
                This may indicate a bug, pathological query, or very large dataset. \
                Consider adding LIMIT or optimizing the query.",
                self.max_duration_secs
            ));
        }
        Ok(())
    }
    
    /// Get elapsed time in seconds
    pub fn elapsed_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_operator_contracts() {
        let mut contracts = OperatorContracts::new();
        
        // Record some batches
        for _ in 0..100 {
            contracts.record_batch(100).unwrap();
        }
        
        assert_eq!(contracts.total_batches, 100);
        assert_eq!(contracts.consecutive_empty_batches, 0);
    }
    
    #[test]
    fn test_empty_batch_limit() {
        let mut contracts = OperatorContracts::with_limits(1000, 10);
        
        // Record empty batches
        for _ in 0..10 {
            contracts.record_batch(0).unwrap();
        }
        
        // 11th empty batch should fail
        assert!(contracts.record_batch(0).is_err());
    }
    
    #[test]
    fn test_timeout() {
        let timeout = ExecutionTimeout::new(1);
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(timeout.check().is_ok());
    }
}

