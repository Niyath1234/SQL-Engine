/// Work-Stealing Runtime for Parallel Execution
/// 
/// This module provides a work-stealing task queue for dynamic load balancing
/// across multiple threads. It enables efficient parallel execution of operators
/// with automatic load balancing.
use std::sync::Arc;
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Task to be executed
pub trait Task: Send + Sync {
    /// Execute the task
    fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get task priority (higher = more important)
    fn priority(&self) -> usize {
        0
    }
}

/// Work-stealing queue
pub struct WorkStealingQueue {
    /// Tasks in the queue
    tasks: Arc<Mutex<VecDeque<Box<dyn Task>>>>,
    
    /// Number of workers
    num_workers: usize,
}

impl WorkStealingQueue {
    /// Create a new work-stealing queue
    pub fn new(num_workers: usize) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(VecDeque::new())),
            num_workers,
        }
    }
    
    /// Push a task to the queue
    pub async fn push(&self, task: Box<dyn Task>) {
        let mut tasks = self.tasks.lock().await;
        tasks.push_back(task);
        debug!("Task pushed to work-stealing queue");
    }
    
    /// Pop a task from the queue (for local worker)
    pub async fn pop(&self) -> Option<Box<dyn Task>> {
        let mut tasks = self.tasks.lock().await;
        tasks.pop_front()
    }
    
    /// Steal a task from the queue (for remote worker)
    pub async fn steal(&self) -> Option<Box<dyn Task>> {
        let mut tasks = self.tasks.lock().await;
        tasks.pop_back() // Steal from the back
    }
    
    /// Check if queue is empty
    pub async fn is_empty(&self) -> bool {
        let tasks = self.tasks.lock().await;
        tasks.is_empty()
    }
    
    /// Get queue size
    pub async fn len(&self) -> usize {
        let tasks = self.tasks.lock().await;
        tasks.len()
    }
}

/// Worker pool for parallel execution
pub struct WorkerPool {
    /// Work-stealing queue
    queue: Arc<WorkStealingQueue>,
    
    /// Worker handles
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl WorkerPool {
    /// Create a new worker pool
    pub fn new(num_workers: usize) -> Self {
        let queue = Arc::new(WorkStealingQueue::new(num_workers));
        
        Self {
            queue,
            workers: Vec::new(),
        }
    }
    
    /// Start workers
    pub fn start(&mut self) {
        let queue = Arc::clone(&self.queue);
        let num_workers = self.queue.num_workers;
        
        for worker_id in 0..num_workers {
            let queue_clone = Arc::clone(&queue);
            let handle = tokio::spawn(async move {
                Self::worker_loop(worker_id, queue_clone).await;
            });
            self.workers.push(handle);
        }
        
        info!(
            num_workers = num_workers,
            "Worker pool started"
        );
    }
    
    /// Worker main loop
    async fn worker_loop(worker_id: usize, queue: Arc<WorkStealingQueue>) {
        loop {
            // Try to pop from local queue
            if let Some(mut task) = queue.pop().await {
                debug!(worker = worker_id, "Worker executing task");
                if let Err(e) = task.execute() {
                    tracing::error!(
                        worker = worker_id,
                        error = %e,
                        "Task execution failed"
                    );
                }
                continue;
            }
            
            // Try to steal from other workers
            if let Some(mut task) = queue.steal().await {
                debug!(worker = worker_id, "Worker stole task");
                if let Err(e) = task.execute() {
                    tracing::error!(
                        worker = worker_id,
                        error = %e,
                        "Stolen task execution failed"
                    );
                }
                continue;
            }
            
            // No work available, yield
            tokio::task::yield_now().await;
        }
    }
    
    /// Submit a task to the queue
    pub async fn submit(&self, task: Box<dyn Task>) {
        self.queue.push(task).await;
    }
    
    /// Wait for all workers to finish (for shutdown)
    pub async fn shutdown(self) {
        // In a full implementation, would signal workers to stop
        // For now, just wait for handles
        for handle in self.workers {
            let _ = handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    struct TestTask {
        id: usize,
        executed: bool,
    }
    
    impl Task for TestTask {
        fn execute(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.executed = true;
            Ok(())
        }
    }
    
    #[tokio::test]
    async fn test_work_stealing_queue() {
        let queue = WorkStealingQueue::new(4);
        
        // Push tasks
        for i in 0..10 {
            let task = Box::new(TestTask { id: i, executed: false });
            queue.push(task).await;
        }
        
        assert_eq!(queue.len().await, 10);
        
        // Pop tasks
        for _ in 0..5 {
            let task = queue.pop().await;
            assert!(task.is_some());
        }
        
        assert_eq!(queue.len().await, 5);
    }
}

