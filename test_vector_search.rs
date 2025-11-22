/// Test suite for vector search functionality
/// Tests VECTOR_SIMILARITY() and VECTOR_DISTANCE() functions
use hypergraph_sql_engine::engine::HypergraphSQLEngine;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use anyhow::Result;

/// Create sample vector data for testing
/// Returns (documents table data, queries table data)
fn create_vector_test_data() -> (
    (Vec<String>, Vec<Arc<dyn Array>>),
    (Vec<String>, Vec<Arc<dyn Array>>),
) {
    // Create documents table with embeddings
    // Each document has: id, title, embedding (vector of 3 floats)
    
    let doc_ids = vec!["doc1", "doc2", "doc3", "doc4", "doc5"];
    let doc_titles = vec![
        "Machine Learning Basics",
        "Deep Learning Tutorial",
        "Natural Language Processing",
        "Computer Vision Guide",
        "Neural Networks Explained",
    ];
    
    // Embeddings (simplified 3D vectors for testing)
    // doc1: ML-related (0.8, 0.6, 0.0)
    // doc2: DL-related (0.9, 0.8, 0.1)
    // doc3: NLP-related (0.6, 0.4, 0.8)
    // doc4: CV-related (0.7, 0.5, 0.3)
    // doc5: NN-related (0.85, 0.7, 0.2)
    let embeddings: Vec<Vec<f32>> = vec![
        vec![0.8, 0.6, 0.0],
        vec![0.9, 0.8, 0.1],
        vec![0.6, 0.4, 0.8],
        vec![0.7, 0.5, 0.3],
        vec![0.85, 0.7, 0.2],
    ];
    
    // Normalize embeddings (for cosine similarity testing)
    let normalized_embeddings: Vec<Vec<f32>> = embeddings.iter()
        .map(|emb| {
            let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                emb.iter().map(|x| x / norm).collect()
            } else {
                emb.clone()
            }
        })
        .collect();
    
    // Create Arrow arrays for documents
    let id_array = StringArray::from(
        doc_ids.iter().map(|s| Some(*s)).collect::<Vec<_>>()
    );
    
    let title_array = StringArray::from(
        doc_titles.iter().map(|s| Some(*s)).collect::<Vec<_>>()
    );
    
    // Create FixedSizeListArray for embeddings (vectors)
    let dimension = 3;
    
    // Flatten embeddings into a single array
    let mut embedding_values = Vec::new();
    for emb in &normalized_embeddings {
        for &val in emb {
            embedding_values.push(Some(val));
        }
    }
    
    let embedding_float_array = Float32Array::from(embedding_values);
    
    // Create FixedSizeListArray from the flattened array
    let list_field = Arc::new(Field::new("item", DataType::Float32, true));
    let embedding_array = FixedSizeListArray::try_new(
        list_field.clone(),
        dimension as i32,
        Arc::new(embedding_float_array),
        None, // No null bitmap
    ).unwrap();
    
    // Wrap in Arc<dyn Array>
    let id_array_arc: Arc<dyn Array> = Arc::new(id_array);
    let title_array_arc: Arc<dyn Array> = Arc::new(title_array);
    let embedding_array_arc: Arc<dyn Array> = Arc::new(embedding_array);
    
    let doc_columns = vec![id_array_arc, title_array_arc, embedding_array_arc];
    let doc_column_names = vec!["id".to_string(), "title".to_string(), "embedding".to_string()];
    
    // Create queries table with query vectors
    let query_ids = vec!["q1", "q2"];
    let query_texts = vec![
        "What is machine learning?",
        "How does neural network work?",
    ];
    
    // Query embeddings (also 3D vectors)
    // q1: ML-related (0.85, 0.65, 0.05) - should match doc1, doc2, doc5
    // q2: NN-related (0.88, 0.72, 0.15) - should match doc2, doc5
    let query_embeddings_raw: Vec<Vec<f32>> = vec![
        vec![0.85, 0.65, 0.05],
        vec![0.88, 0.72, 0.15],
    ];
    
    // Normalize query embeddings
    let normalized_query_embeddings: Vec<Vec<f32>> = query_embeddings_raw.iter()
        .map(|emb| {
            let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                emb.iter().map(|x| x / norm).collect()
            } else {
                emb.clone()
            }
        })
        .collect();
    
    let query_id_array = StringArray::from(
        query_ids.iter().map(|s| Some(*s)).collect::<Vec<_>>()
    );
    
    let query_text_array = StringArray::from(
        query_texts.iter().map(|s| Some(*s)).collect::<Vec<_>>()
    );
    
    // Flatten query embeddings
    let mut query_embedding_values = Vec::new();
    for emb in &normalized_query_embeddings {
        for &val in emb {
            query_embedding_values.push(Some(val));
        }
    }
    
    let query_embedding_float_array = Float32Array::from(query_embedding_values);
    let query_list_field = Arc::new(Field::new("item", DataType::Float32, true));
    let query_embedding_array = FixedSizeListArray::try_new(
        query_list_field,
        dimension as i32,
        Arc::new(query_embedding_float_array),
        None,
    ).unwrap();
    
    let query_id_array_arc: Arc<dyn Array> = Arc::new(query_id_array);
    let query_text_array_arc: Arc<dyn Array> = Arc::new(query_text_array);
    let query_embedding_array_arc: Arc<dyn Array> = Arc::new(query_embedding_array);
    
    let query_columns = vec![query_id_array_arc, query_text_array_arc, query_embedding_array_arc];
    let query_column_names = vec!["id".to_string(), "text".to_string(), "embedding".to_string()];
    
    (
        (doc_column_names, doc_columns),
        (query_column_names, query_columns),
    )
}

/// Create ColumnFragments from Arrow arrays
fn create_fragments(column_names: Vec<String>, columns: Vec<Arc<dyn Array>>) -> Vec<(String, hypergraph_sql_engine::storage::fragment::ColumnFragment)> {
    let mut fragments = Vec::new();
    
    for (name, array) in column_names.iter().zip(columns.iter()) {
        let row_count = array.len();
        let fragment = hypergraph_sql_engine::storage::fragment::ColumnFragment::new(
            array.clone(),
            hypergraph_sql_engine::storage::fragment::FragmentMetadata {
                row_count,
                min_value: None,
                max_value: None,
                cardinality: row_count,
                compression: hypergraph_sql_engine::storage::fragment::CompressionType::None,
                memory_size: 0,
            },
        );
        fragments.push((name.clone(), fragment));
    }
    
    fragments
}

fn print_result(test_name: &str, result: &hypergraph_sql_engine::execution::engine::QueryResult) {
    println!("\n {}: SUCCESS", test_name);
    println!("   Rows: {}", result.row_count);
    println!("   Execution time: {:.2} ms", result.execution_time_ms);
    
    if result.row_count > 0 && !result.batches.is_empty() {
        let first_batch = &result.batches[0].batch;
        println!("   Sample output (first {} rows):", result.row_count.min(5));
        
        for row_idx in 0..(result.row_count.min(5)) {
            print!("     Row {}: ", row_idx + 1);
            let mut values = Vec::new();
            
            for (col_idx, field) in first_batch.schema.fields().iter().enumerate() {
                if col_idx < first_batch.columns.len() {
                    let col = &first_batch.columns[col_idx];
                    let val_str = if col.len() > row_idx {
                        if col.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            match col.data_type() {
                                DataType::Int64 => {
                                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                    format!("{}", arr.value(row_idx))
                                }
                                DataType::Float64 => {
                                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                                    format!("{:.4}", arr.value(row_idx))
                                }
                                DataType::Utf8 => {
                                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                    format!("\"{}\"", arr.value(row_idx))
                                }
                                _ => "?".to_string(),
                            }
                        }
                    } else {
                        "N/A".to_string()
                    };
                    values.push(format!("{}: {}", field.name(), val_str));
                }
            }
            println!("{}", values.join(", "));
        }
    }
}

fn print_error(test_name: &str, error: &anyhow::Error) {
    println!("\n {}: FAILED", test_name);
    println!("   Error: {}", error);
    if let Some(chain_err) = error.chain().skip(1).next() {
        println!("   Caused by: {}", chain_err);
    }
}

fn main() -> Result<()> {
    println!(" Vector Search Functionality Test Suite");
    println!("{}", "=".repeat(70));
    
    // Initialize engine
    println!("\n¦ Initializing Hypergraph SQL Engine...");
    let mut engine = HypergraphSQLEngine::new();
    
    // Create test data
    println!("\n Creating test data...");
    let (doc_data, query_data) = create_vector_test_data();
    
    let doc_fragments = create_fragments(doc_data.0, doc_data.1);
    let query_fragments = create_fragments(query_data.0, query_data.1);
    
    println!("   Created {} documents with embeddings", doc_fragments[0].1.len());
    println!("   Created {} queries with embeddings", query_fragments[0].1.len());
    
    // Load documents table
    println!("\n Loading 'documents' table...");
    engine.load_table("documents", doc_fragments)?;
    println!("   Documents loaded successfully");
    
    // Load queries table
    println!("\n Loading 'queries' table...");
    engine.load_table("queries", query_fragments)?;
    println!("   Queries loaded successfully");
    
    let mut test_count = 0;
    let mut pass_count = 0;
    
    // Test 1: Basic SELECT from documents
    test_count += 1;
    println!("\n Test {}: Basic SELECT from documents", test_count);
    match engine.execute_query("SELECT id, title FROM documents LIMIT 3") {
        Ok(result) => {
            print_result(&format!("Test {}: Basic SELECT", test_count), &result);
            if result.row_count > 0 {
                pass_count += 1;
            }
        }
        Err(e) => {
            print_error(&format!("Test {}: Basic SELECT", test_count), &e);
        }
    }
    
    // Test 2: VECTOR_SIMILARITY with fixed query vector
    test_count += 1;
    println!("\n Test {}: VECTOR_SIMILARITY with fixed query vector", test_count);
    // Note: For now, we'll test with a simple approach since vector literals aren't fully implemented
    // We'll use a subquery or join to get a vector from the queries table
    match engine.execute_query(
        "SELECT d.id, d.title, \
         VECTOR_SIMILARITY(d.embedding, (SELECT embedding FROM queries WHERE id = 'q1' LIMIT 1)) AS similarity \
         FROM documents d \
         ORDER BY similarity DESC \
         LIMIT 3"
    ) {
        Ok(result) => {
            print_result(&format!("Test {}: VECTOR_SIMILARITY", test_count), &result);
            if result.row_count > 0 {
                pass_count += 1;
            }
        }
        Err(e) => {
            print_error(&format!("Test {}: VECTOR_SIMILARITY", test_count), &e);
            println!("   Note: Subquery in SELECT might not be fully supported yet");
        }
    }
    
    // Test 3: VECTOR_SIMILARITY with cross join
    test_count += 1;
    println!("\n Test {}: VECTOR_SIMILARITY with CROSS JOIN", test_count);
    match engine.execute_query(
        "SELECT d.id AS doc_id, d.title, q.id AS query_id, \
         VECTOR_SIMILARITY(d.embedding, q.embedding) AS similarity \
         FROM documents d \
         CROSS JOIN queries q \
         ORDER BY similarity DESC \
         LIMIT 5"
    ) {
        Ok(result) => {
            print_result(&format!("Test {}: VECTOR_SIMILARITY with JOIN", test_count), &result);
            if result.row_count > 0 {
                pass_count += 1;
            }
        }
        Err(e) => {
            print_error(&format!("Test {}: VECTOR_SIMILARITY with JOIN", test_count), &e);
        }
    }
    
    // Test 4: VECTOR_DISTANCE function
    test_count += 1;
    println!("\n Test {}: VECTOR_DISTANCE function", test_count);
    match engine.execute_query(
        "SELECT d.id AS doc_id, q.id AS query_id, \
         VECTOR_DISTANCE(d.embedding, q.embedding) AS distance \
         FROM documents d \
         CROSS JOIN queries q \
         ORDER BY distance ASC \
         LIMIT 5"
    ) {
        Ok(result) => {
            print_result(&format!("Test {}: VECTOR_DISTANCE", test_count), &result);
            if result.row_count > 0 {
                pass_count += 1;
            }
        }
        Err(e) => {
            print_error(&format!("Test {}: VECTOR_DISTANCE", test_count), &e);
        }
    }
    
    // Test 5: VECTOR_SIMILARITY with WHERE clause
    test_count += 1;
    println!("\n Test {}: VECTOR_SIMILARITY with WHERE clause", test_count);
    match engine.execute_query(
        "SELECT d.id, d.title, \
         VECTOR_SIMILARITY(d.embedding, (SELECT embedding FROM queries WHERE id = 'q1' LIMIT 1)) AS similarity \
         FROM documents d \
         WHERE VECTOR_SIMILARITY(d.embedding, (SELECT embedding FROM queries WHERE id = 'q1' LIMIT 1)) > 0.9 \
         ORDER BY similarity DESC"
    ) {
        Ok(result) => {
            print_result(&format!("Test {}: VECTOR_SIMILARITY with WHERE", test_count), &result);
            pass_count += 1; // Even if no results, the query executed successfully
        }
        Err(e) => {
            print_error(&format!("Test {}: VECTOR_SIMILARITY with WHERE", test_count), &e);
            println!("   Note: Subquery in WHERE might not be fully supported yet");
        }
    }
    
    // Test 6: Simple similarity calculation (using cross join)
    test_count += 1;
    println!("\n Test {}: Similarity with WHERE on similarity threshold", test_count);
    match engine.execute_query(
        "SELECT d.id AS doc_id, d.title, q.id AS query_id, \
         VECTOR_SIMILARITY(d.embedding, q.embedding) AS similarity \
         FROM documents d \
         CROSS JOIN queries q \
         WHERE q.id = 'q1' \
         ORDER BY similarity DESC \
         LIMIT 3"
    ) {
        Ok(result) => {
            print_result(&format!("Test {}: Similarity with WHERE", test_count), &result);
            if result.row_count > 0 {
                pass_count += 1;
            }
        }
        Err(e) => {
            print_error(&format!("Test {}: Similarity with WHERE", test_count), &e);
        }
    }
    
    // Summary
    println!("\n{}", "=".repeat(70));
    println!(" Test Summary");
    println!("{}", "=".repeat(70));
    println!("   Total tests: {}", test_count);
    println!("   Passed: {}", pass_count);
    println!("   Failed: {}", test_count - pass_count);
    println!("   Success rate: {:.1}%", (pass_count as f64 / test_count as f64) * 100.0);
    
    if pass_count == test_count {
        println!("\n All tests passed!");
    } else {
        println!("\n  Some tests failed - see details above");
    }
    
    Ok(())
}

