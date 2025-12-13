/// Streaming Result Delivery
/// 
/// This module implements Phase 9: Streaming Result Delivery.
/// It provides streaming HTTP responses using Arrow IPC format and chunked JSON transfer.
use crate::execution::batch::ExecutionBatch;
use crate::execution::engine::QueryResult;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use arrow::ipc::reader::StreamReader;
use arrow::array::*;
use arrow::datatypes::*;
use axum::{
    body::Body,
    response::{Response, IntoResponse},
    http::{StatusCode, HeaderMap, HeaderValue},
};
use serde_json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{debug, info, warn};

/// Streaming response format
#[derive(Debug, Clone, Copy)]
pub enum StreamingFormat {
    /// Arrow IPC streaming format
    ArrowIpc,
    /// Chunked JSON format
    JsonChunked,
    /// Traditional JSON (non-streaming, for backward compatibility)
    Json,
}

/// Cancellation token for query execution
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<tokio::sync::Mutex<bool>>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }
    
    pub async fn cancel(&self) {
        let mut cancelled = self.cancelled.lock().await;
        *cancelled = true;
    }
    
    pub async fn is_cancelled(&self) -> bool {
        let cancelled = self.cancelled.lock().await;
        *cancelled
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert ExecutionBatch to Arrow RecordBatch
fn execution_batch_to_record_batch(batch: &ExecutionBatch) -> arrow::error::Result<RecordBatch> {
    let schema = batch.batch.schema.clone();
    let mut arrays = Vec::new();
    
    for col_idx in 0..batch.batch.columns.len() {
        let array = batch.batch.column(col_idx)
            .ok_or_else(|| arrow::error::ArrowError::InvalidArgumentError(
                format!("Column {} not found", col_idx)
            ))?;
        arrays.push(array.clone());
    }
    
    RecordBatch::try_new(schema, arrays)
}

/// Stream query results as Arrow IPC format
pub async fn stream_arrow_ipc(
    batches: impl Stream<Item = Result<ExecutionBatch, String>> + Send + Unpin + 'static,
    cancellation: CancellationToken,
) -> Response<Body> {
    let mut batches = Box::pin(batches);
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, String>>(10);
    
    // Spawn task to convert batches to Arrow IPC stream
    tokio::spawn(async move {
        let mut buffer = Vec::new();
        let mut writer: Option<StreamWriter<&mut Vec<u8>>> = None;
        
        loop {
            // Check cancellation
            if cancellation.is_cancelled().await {
                debug!("Query cancelled, stopping Arrow IPC stream");
                break;
            }
            
            // Get next batch
            match batches.next().await {
                Some(Ok(batch)) => {
                    // Convert to RecordBatch
                    match execution_batch_to_record_batch(&batch) {
                        Ok(record_batch) => {
                            // Initialize writer on first batch
                            if writer.is_none() {
                                let schema = record_batch.schema();
                                
                                match StreamWriter::try_new(&mut buffer, schema.as_ref()) {
                                    Ok(w) => {
                                        writer = Some(w);
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(format!("Failed to create Arrow IPC writer: {}", e))).await;
                                        break;
                                    }
                                }
                            }
                            
                            // Write batch to stream
                            if let Some(ref mut w) = writer {
                                match w.write(&record_batch) {
                                    Ok(_) => {
                                        // For now, we accumulate and send at the end
                                        // In a full implementation, we'd use a streaming writer
                                        // that allows extracting chunks without finishing
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(format!("Failed to write Arrow IPC batch: {}", e))).await;
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(format!("Failed to convert batch: {}", e))).await;
                            break;
                        }
                    }
                }
                Some(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
                None => {
                    // End of stream - finalize writer and send all data
                    if let Some(mut w) = writer {
                        if let Err(e) = w.finish() {
                            let _ = tx.send(Err(format!("Failed to finalize Arrow IPC writer: {}", e))).await;
                        } else {
                            // Send final buffer
                            if !buffer.is_empty() {
                                let _ = tx.send(Ok(bytes::Bytes::from(buffer))).await;
                            }
                        }
                    }
                    break;
                }
            }
        }
    });
    
    // Create streaming response
    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    headers.insert(
        axum::http::header::TRANSFER_ENCODING,
        HeaderValue::from_static("chunked"),
    );
    
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")
        .header(axum::http::header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .unwrap()
}

/// Stream query results as chunked JSON
pub async fn stream_json_chunked(
    batches: impl Stream<Item = Result<ExecutionBatch, String>> + Send + Unpin + 'static,
    cancellation: CancellationToken,
) -> Response<Body> {
    let mut batches = Box::pin(batches);
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, String>>(10);
    
    // Spawn task to convert batches to JSON chunks
    tokio::spawn(async move {
        let mut first_chunk = true;
        let mut columns: Vec<String> = Vec::new();
        
        // Send opening bracket
        if tx.send(Ok(bytes::Bytes::from("{\"columns\":["))).await.is_err() {
            return;
        }
        
        loop {
            // Check cancellation
            if cancellation.is_cancelled().await {
                debug!("Query cancelled, stopping JSON stream");
                break;
            }
            
            // Get next batch
            match batches.next().await {
                Some(Ok(batch)) => {
                    // Extract columns from first batch
                    if first_chunk && !batch.batch.columns.is_empty() {
                        let schema = batch.batch.schema.clone();
                        for field in schema.fields() {
                            columns.push(field.name().clone());
                        }
                        
                        // Send column names
                        let columns_json = serde_json::to_string(&columns)
                            .unwrap_or_else(|_| "[]".to_string());
                        if tx.send(Ok(bytes::Bytes::from(format!("\"rows\":[{}", columns_json)))).await.is_err() {
                            break;
                        }
                        first_chunk = false;
                    }
                    
                    // Convert batch rows to JSON
                    let col_count = columns.len();
                    if col_count == 0 || batch.batch.columns.is_empty() {
                        continue;
                    }
                    
                    let total_rows = batch.selection.len();
                    for row_idx in 0..total_rows {
                        if !batch.selection[row_idx] {
                            continue;
                        }
                        
                        let mut row = Vec::with_capacity(col_count);
                        for col_idx in 0..col_count {
                            if let Some(array) = batch.batch.column(col_idx) {
                                let value = format_value_from_array(array, row_idx);
                                row.push(value);
                            } else {
                                row.push("null".to_string());
                            }
                        }
                        
                        let row_json = serde_json::to_string(&row)
                            .unwrap_or_else(|_| "[]".to_string());
                        
                        // Send row as chunk
                        let chunk = format!(",{}", row_json);
                        if tx.send(Ok(bytes::Bytes::from(chunk))).await.is_err() {
                            return;
                        }
                    }
                }
                Some(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
                None => {
                    // End of stream - send closing brackets
                    let _ = tx.send(Ok(bytes::Bytes::from("]}"))).await;
                    break;
                }
            }
        }
    });
    
    // Create streaming response
    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .header(axum::http::header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .unwrap()
}

/// Format value from Arrow array at index
fn format_value_from_array(array: &Arc<dyn Array>, idx: usize) -> String {
    if array.is_null(idx) {
        return "null".to_string();
    }
    
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .expect("Type mismatch");
            arr.value(idx).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .expect("Type mismatch");
            arr.value(idx).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .expect("Type mismatch");
            serde_json::to_string(arr.value(idx)).unwrap_or_else(|_| "null".to_string())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .expect("Type mismatch");
            arr.value(idx).to_string()
        }
        _ => format!("{:?}", array)
    }
}

/// Convert QueryResult to streaming batches
pub fn query_result_to_stream(
    result: QueryResult,
) -> impl Stream<Item = Result<ExecutionBatch, String>> + Send + Unpin {
    use tokio_stream::StreamExt;
    
    tokio_stream::iter(result.batches.into_iter().map(Ok))
}

