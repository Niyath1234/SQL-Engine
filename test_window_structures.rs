// Quick test script to verify window function structures compile
use hypergraph_sql_engine::query::parser::{WindowFunctionInfo, WindowFunctionType};
use hypergraph_sql_engine::query::plan::{WindowFunctionExpr, WindowFunction};

fn main() {
    // Test WindowFunctionType
    let win_type = WindowFunctionType::RowNumber;
    match win_type {
        WindowFunctionType::RowNumber => println!(" WindowFunctionType::RowNumber works"),
        _ => panic!("Failed"),
    }
    
    // Test WindowFunctionInfo
    let win_info = WindowFunctionInfo {
        function: WindowFunctionType::RowNumber,
        column: None,
        alias: Some("rn".to_string()),
        partition_by: vec!["year".to_string()],
        order_by: vec![],
        frame: None,
    };
    println!(" WindowFunctionInfo created successfully");
    
    // Test WindowFunction
    let win_func = WindowFunction::RowNumber;
    match win_func {
        WindowFunction::RowNumber => println!(" WindowFunction::RowNumber works"),
        _ => panic!("Failed"),
    }
    
    // Test WindowFunctionExpr
    let win_expr = WindowFunctionExpr {
        function: WindowFunction::RowNumber,
        column: None,
        alias: Some("rn".to_string()),
        partition_by: vec!["year".to_string()],
        order_by: vec![],
        frame: None,
    };
    println!(" WindowFunctionExpr created successfully");
    
    println!("\n All window function structures compile and work correctly!");
}
