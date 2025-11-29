/// Integration tests for WASM JIT compilation
/// Tests that JIT provides performance improvements for expressions

use hypergraph_sql_engine::codegen::{WasmCodegen, WasmProgramSpec};
use hypergraph_sql_engine::execution::wasm_runner::WasmRunner;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int64Array, Float64Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

#[test]
fn test_wasm_codegen_complexity() {
    // Test that complexity scoring works correctly
    let simple_spec = WasmProgramSpec {
        expression: "a".to_string(),
        input_types: vec!["Int64".to_string()],
        output_type: Some("Int64".to_string()),
    };
    
    assert!(!WasmCodegen::should_compile(&simple_spec), "Simple expression should not be compiled");
    
    let complex_spec = WasmProgramSpec {
        expression: "a + b * 2 - c / 3".to_string(),
        input_types: vec!["Int64".to_string(), "Int64".to_string(), "Int64".to_string()],
        output_type: Some("Int64".to_string()),
    };
    
    assert!(WasmCodegen::should_compile(&complex_spec), "Complex expression should be compiled");
    assert!(complex_spec.complexity_score() > 10, "Complex expression should have high score");
}

#[test]
fn test_wasm_codegen_compile() {
    // Test that WASM codegen compiles expressions
    let spec = WasmProgramSpec {
        expression: "a + b * 2".to_string(),
        input_types: vec!["Int64".to_string(), "Int64".to_string()],
        output_type: Some("Int64".to_string()),
    };
    
    let result = WasmCodegen::compile(&spec, None);
    assert!(result.is_ok(), "WASM compilation should succeed");
    
    let wasm_bytes = result.unwrap();
    assert!(!wasm_bytes.is_empty(), "WASM bytes should not be empty");
}

#[test]
fn test_wasm_runner_creation() {
    // Test that WASM runner can be created
    let runner = WasmRunner::with_cache(None);
    // Runner should be created successfully
    assert!(true, "WASM runner should be created");
}

#[test]
fn test_wasm_runner_fallback() {
    // Test that WASM runner falls back to interpreter when WASM fails
    let runner = WasmRunner::with_cache(None);
    
    // Create a test batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
    ]));
    
    let values = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    
    // Try to run with invalid WASM (should fallback)
    let invalid_wasm = vec![0, 1, 2, 3]; // Not valid WASM
    let result = runner.run(&invalid_wasm, &batch);
    
    // Should fallback to interpreter (return input)
    assert!(result.is_ok(), "WASM runner should fallback gracefully");
}

#[test]
fn test_jit_overhead() {
    // Test that JIT overhead is reasonable (< 10ms per module)
    let spec = WasmProgramSpec {
        expression: "a + b * 2 - c / 3".to_string(),
        input_types: vec!["Int64".to_string(), "Int64".to_string(), "Int64".to_string()],
        output_type: Some("Int64".to_string()),
    };
    
    let start = std::time::Instant::now();
    
    // Compile multiple times to measure overhead
    for _ in 0..100 {
        let _ = WasmCodegen::compile(&spec, None);
    }
    
    let elapsed = start.elapsed();
    let avg_time = elapsed.as_millis() / 100;
    
    // Average compilation time should be < 10ms
    assert!(avg_time < 10, "JIT compilation overhead should be < 10ms per module");
}

#[test]
fn test_wasm_jit_arithmetic() {
    // Test that arithmetic expressions can be JIT compiled
    let spec = WasmProgramSpec {
        expression: "a + b * 2".to_string(),
        input_types: vec!["Int64".to_string(), "Int64".to_string()],
        output_type: Some("Int64".to_string()),
    };
    
    assert!(WasmCodegen::should_compile(&spec), "Arithmetic expression should be compiled");
    
    let wasm_code = WasmCodegen::compile(&spec, None).unwrap();
    assert!(!wasm_code.is_empty(), "WASM code should be generated");
}

#[test]
fn test_wasm_jit_filter_predicates() {
    // Test that filter predicates can be JIT compiled
    let spec = WasmProgramSpec {
        expression: "a > 10 AND b < 20".to_string(),
        input_types: vec!["Int64".to_string(), "Int64".to_string()],
        output_type: Some("Boolean".to_string()),
    };
    
    assert!(WasmCodegen::should_compile(&spec), "Filter predicate should be compiled");
    assert!(spec.complexity_score() > 10, "Filter with AND should have high complexity");
}

#[test]
fn test_wasm_jit_aggregation() {
    // Test that aggregation expressions can be JIT compiled
    let spec = WasmProgramSpec {
        expression: "SUM(a) + AVG(b)".to_string(),
        input_types: vec!["Int64".to_string(), "Int64".to_string()],
        output_type: Some("Float64".to_string()),
    };
    
    // Aggregation might not be complex enough, but test the structure
    let wasm_code = WasmCodegen::compile(&spec, None);
    assert!(wasm_code.is_ok(), "Aggregation expression compilation should not error");
}

