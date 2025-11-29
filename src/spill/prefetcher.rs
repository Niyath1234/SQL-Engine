/// Async prefetcher for spill files
/// Prefetches spill files asynchronously to reduce merge latency
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use anyhow::Result;
use std::path::Path;

/// Prefetch a spill file asynchronously
/// Reads the entire file into memory for fast access during merge
pub async fn prefetch(path: &str) -> Result<Vec<u8>> {
    let mut f = File::open(path).await?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).await?;
    Ok(buf)
}

/// Prefetch multiple spill files in parallel
pub async fn prefetch_multiple(paths: &[impl AsRef<Path>]) -> Result<Vec<Vec<u8>>> {
    use tokio::task;
    
    let mut handles = vec![];
    for path in paths {
        let path_str = path.as_ref().to_string_lossy().to_string();
        handles.push(task::spawn(async move {
            prefetch(&path_str).await
        }));
    }
    
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await??);
    }
    
    Ok(results)
}

/// Prefetch spill files with progress tracking
pub async fn prefetch_with_progress(
    paths: &[impl AsRef<Path>],
    progress_callback: impl Fn(usize, usize) + Send + Sync + Clone + 'static,
) -> Result<Vec<Vec<u8>>> {
    use tokio::task;
    
    let total = paths.len();
    let mut handles = vec![];
    
    for (idx, path) in paths.iter().enumerate() {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let callback = progress_callback.clone();
        handles.push(task::spawn(async move {
            let result = prefetch(&path_str).await;
            callback(idx + 1, total);
            result
        }));
    }
    
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await??);
    }
    
    Ok(results)
}

