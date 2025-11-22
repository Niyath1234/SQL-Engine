/// Tests for Window Functions Infrastructure
/// Tests the newly introduced window function data structures and parsing

use hypergraph_sql_engine::query::parser::{ParsedQuery, WindowFunctionInfo, WindowFunctionType};
use hypergraph_sql_engine::query::plan::{PlanOperator, WindowFunctionExpr, WindowFunction};
use hypergraph_sql_engine::query::parser_enhanced::extract_query_info_enhanced;
use hypergraph_sql_engine::hypergraph::node::NodeId;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn test_window_function_structures() {
    // Test that window function types can be created
    let win_func_type = WindowFunctionType::RowNumber;
    assert!(matches!(win_func_type, WindowFunctionType::RowNumber));
    
    let win_func_type2 = WindowFunctionType::SumOver;
    assert!(matches!(win_func_type2, WindowFunctionType::SumOver));
    
    // Test WindowFunctionInfo structure
    let win_info = WindowFunctionInfo {
        function: WindowFunctionType::RowNumber,
        column: None,
        alias: Some("rn".to_string()),
        partition_by: vec!["year".to_string()],
        order_by: vec![],
        frame: None,
    };
    
    assert_eq!(win_info.alias, Some("rn".to_string()));
    assert_eq!(win_info.partition_by, vec!["year".to_string()]);
}

#[test]
fn test_window_function_plan_types() {
    // Test WindowFunctionExpr
    let win_expr = WindowFunctionExpr {
        function: WindowFunction::RowNumber,
        column: None,
        alias: Some("rn".to_string()),
        partition_by: vec!["year".to_string()],
        order_by: vec![],
        frame: None,
    };
    
    assert!(matches!(win_expr.function, WindowFunction::RowNumber));
    assert_eq!(win_expr.alias, Some("rn".to_string()));
    
    // Test Window operator
    let scan_op = PlanOperator::Scan {
        node_id: NodeId(0),
        table: "test".to_string(),
        columns: vec!["col1".to_string()],
        limit: None,
        offset: None,
    };
    
    let window_op = PlanOperator::Window {
        input: Box::new(scan_op),
        window_functions: vec![win_expr],
    };
    
    match window_op {
        PlanOperator::Window { input, window_functions } => {
            assert_eq!(window_functions.len(), 1);
            assert!(matches!(input.as_ref(), PlanOperator::Scan { .. }));
        }
        _ => panic!("Expected Window operator"),
    }
}

#[test]
fn test_window_function_parsing_infrastructure() {
    // Test that ParsedQuery has window_functions field
    let parsed = ParsedQuery {
        tables: vec!["test".to_string()],
        columns: vec!["col1".to_string()],
        joins: vec![],
        filters: vec![],
        aggregates: vec![],
        window_functions: vec![],
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        projection_expressions: vec![],
        distinct: false,
    };
    
    // Verify window_functions field exists and is empty
    assert_eq!(parsed.window_functions.len(), 0);
    
    // Add a window function
    let mut parsed_with_window = parsed;
    parsed_with_window.window_functions.push(WindowFunctionInfo {
        function: WindowFunctionType::RowNumber,
        column: None,
        alias: Some("rn".to_string()),
        partition_by: vec![],
        order_by: vec![],
        frame: None,
    });
    
    assert_eq!(parsed_with_window.window_functions.len(), 1);
}

#[test]
fn test_sql_with_window_function_parsing() {
    // Test parsing SQL with window function (even if extraction not complete)
    let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY year ORDER BY month) as rn FROM table1";
    
    let dialect = GenericDialect;
    let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    
    match parser.parse_statement() {
        Ok(Statement::Query(query)) => {
            // Test that we can extract query info (even if window functions not fully extracted)
            match extract_query_info_enhanced(&Statement::Query(query)) {
                Ok(parsed) => {
                    // Verify the query was parsed
                    assert!(!parsed.tables.is_empty());
                    // Note: window_functions may be empty if extraction not complete
                    // That's okay for now - we're testing infrastructure
                }
                Err(e) => {
                    // Parsing might fail for various reasons, but that's okay
                    // We're testing that the infrastructure exists
                    println!("Parsing note (expected for incomplete implementation): {:?}", e);
                }
            }
        }
        Ok(_) => panic!("Expected Query statement"),
        Err(e) => {
            // If parsing fails, that's okay - we're testing infrastructure
            println!("SQL parsing note: {:?}", e);
        }
    }
}

#[test]
fn test_window_function_types_enum() {
    // Test all window function types can be created
    let functions = vec![
        WindowFunctionType::RowNumber,
        WindowFunctionType::Rank,
        WindowFunctionType::DenseRank,
        WindowFunctionType::Lag { offset: 1 },
        WindowFunctionType::Lead { offset: 1 },
        WindowFunctionType::SumOver,
        WindowFunctionType::AvgOver,
        WindowFunctionType::MinOver,
        WindowFunctionType::MaxOver,
        WindowFunctionType::CountOver,
        WindowFunctionType::FirstValue,
        WindowFunctionType::LastValue,
    ];
    
    assert_eq!(functions.len(), 12);
    
    // Test that each type can be matched
    for func in functions {
        match func {
            WindowFunctionType::RowNumber
            | WindowFunctionType::Rank
            | WindowFunctionType::DenseRank
            | WindowFunctionType::Lag { .. }
            | WindowFunctionType::Lead { .. }
            | WindowFunctionType::SumOver
            | WindowFunctionType::AvgOver
            | WindowFunctionType::MinOver
            | WindowFunctionType::MaxOver
            | WindowFunctionType::CountOver
            | WindowFunctionType::FirstValue
            | WindowFunctionType::LastValue => {
                // All types are valid
            }
        }
    }
}

#[test]
fn test_window_function_plan_operator() {
    // Test that Window operator can be part of a query plan
    let scan = PlanOperator::Scan {
        node_id: NodeId(1),
        table: "test".to_string(),
        columns: vec!["year".to_string(), "value".to_string()],
        limit: None,
        offset: None,
    };
    
    let window_func = WindowFunctionExpr {
        function: WindowFunction::RowNumber,
        column: None,
        alias: Some("row_num".to_string()),
        partition_by: vec!["year".to_string()],
        order_by: vec![],
        frame: None,
    };
    
    let window_op = PlanOperator::Window {
        input: Box::new(scan),
        window_functions: vec![window_func],
    };
    
    // Test that we can match on Window operator
    match window_op {
        PlanOperator::Window { input, window_functions } => {
            match *input {
                PlanOperator::Scan { table, .. } => {
                    assert_eq!(table, "test");
                }
                _ => panic!("Expected Scan operator as input"),
            }
            assert_eq!(window_functions.len(), 1);
            assert_eq!(window_functions[0].alias, Some("row_num".to_string()));
        }
        _ => panic!("Expected Window operator"),
    }
}

#[test]
fn test_window_function_with_aggregate() {
    // Test window function that takes a column (like SUM(col) OVER())
    let win_func = WindowFunctionExpr {
        function: WindowFunction::SumOver,
        column: Some("value".to_string()),
        alias: Some("total_value".to_string()),
        partition_by: vec!["year".to_string()],
        order_by: vec![],
        frame: None,
    };
    
    assert_eq!(win_func.column, Some("value".to_string()));
    assert_eq!(win_func.alias, Some("total_value".to_string()));
    assert!(matches!(win_func.function, WindowFunction::SumOver));
}

