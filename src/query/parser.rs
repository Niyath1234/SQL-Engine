use sqlparser::ast::*;
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use anyhow::Result;

/// Parse SQL query into AST
pub fn parse_sql(query: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(query)?;
    
    parser
        .parse_statement()
        .map_err(|e| {
            // CRITICAL FIX: Improve error messages with helpful context
            let error_msg = format!("Failed to parse SQL query: {}", e);
            
            // Provide suggestions for common errors
            let mut suggestions = Vec::new();
            
            if query.trim().is_empty() {
                suggestions.push("Query is empty. Please provide a valid SQL query.");
            } else if !query.trim().to_uppercase().starts_with("SELECT") 
                && !query.trim().to_uppercase().starts_with("INSERT")
                && !query.trim().to_uppercase().starts_with("UPDATE")
                && !query.trim().to_uppercase().starts_with("DELETE")
                && !query.trim().to_uppercase().starts_with("CREATE")
                && !query.trim().to_uppercase().starts_with("DESCRIBE")
                && !query.trim().to_uppercase().starts_with("SHOW") {
                suggestions.push("Query should start with SELECT, INSERT, UPDATE, DELETE, CREATE, DESCRIBE, or SHOW.");
            }
            
            if query.contains("'") && !query.contains("'") {
                suggestions.push("Check for mismatched quotes (single quotes ' or double quotes \").");
            }
            
            if query.matches("(").count() != query.matches(")").count() {
                suggestions.push("Check for mismatched parentheses.");
            }
            
            let full_error = if suggestions.is_empty() {
                format!("{}\n\nQuery: {}", error_msg, query)
            } else {
                format!("{}\n\nSuggestions:\n{}\n\nQuery: {}", 
                    error_msg,
                    suggestions.join("\n"),
                    query)
            };
            
            anyhow::anyhow!(full_error)
        })
}

/// Extract query information from AST
pub struct ParsedQuery {
    pub tables: Vec<String>,
    pub columns: Vec<String>,
    pub joins: Vec<JoinInfo>,
    pub filters: Vec<FilterInfo>,
    pub aggregates: Vec<AggregateInfo>,
    pub window_functions: Vec<WindowFunctionInfo>,
    pub group_by: Vec<String>,
    pub having: Option<String>, // HAVING clause (simplified for now)
    pub order_by: Vec<OrderByInfo>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// Projection expressions (for CAST, functions, etc.)
    pub projection_expressions: Vec<ProjectionExprInfo>,
    /// Whether SELECT DISTINCT is used
    pub distinct: bool,
    /// Table alias mapping: alias -> actual table name (e.g., "d" -> "documents")
    pub table_aliases: std::collections::HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct ProjectionExprInfo {
    pub alias: String,
    pub expr_type: ProjectionExprTypeInfo,
}

#[derive(Clone, Debug)]
pub enum ProjectionExprTypeInfo {
    Column(String),
    Cast {
        column: String,
        target_type: arrow::datatypes::DataType,
    },
    Case(crate::query::expression::Expression),
    Expression(crate::query::expression::Expression), // For function expressions like VECTOR_SIMILARITY
}

#[derive(Clone, Debug)]
pub struct JoinInfo {
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
    pub join_type: JoinType,
}

#[derive(Clone, Debug)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Clone, Debug)]
pub struct FilterInfo {
    pub table: String,
    pub column: String,
    pub operator: FilterOperator,
    pub value: String,
    /// For IN/NOT IN: list of values to check against
    pub in_values: Option<Vec<String>>,
    /// For LIKE/NOT LIKE: pattern string (supports % and _ wildcards)
    pub pattern: Option<String>,
}

#[derive(Clone, Debug)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Between,
    In,
    NotIn,
    Like,
    NotLike,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug)]
pub struct AggregateInfo {
    pub function: AggregateFunction,
    pub column: String,
    pub alias: Option<String>,
    /// Target data type if column is wrapped in CAST expression
    pub cast_type: Option<arrow::datatypes::DataType>,
}

#[derive(Clone, Debug)]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Window function information
#[derive(Clone, Debug)]
pub struct WindowFunctionInfo {
    pub function: WindowFunctionType,
    pub column: Option<String>, // Column for aggregate windows (SUM(col) OVER()), None for ROW_NUMBER, RANK, etc.
    pub alias: Option<String>,
    /// Window specification: PARTITION BY and ORDER BY
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderByInfo>,
    /// Frame specification (ROWS BETWEEN ... AND ...)
    pub frame: Option<WindowFrame>,
}

#[derive(Clone, Debug)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    Lag { offset: usize },
    Lead { offset: usize },
    SumOver,
    AvgOver,
    MinOver,
    MaxOver,
    CountOver,
    FirstValue,
    LastValue,
}

#[derive(Clone, Debug)]
pub struct WindowFrame {
    pub frame_type: FrameType,
    pub start: FrameBound,
    pub end: Option<FrameBound>,
}

#[derive(Clone, Debug)]
pub enum FrameType {
    Rows,
    Range,
}

#[derive(Clone, Debug)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

#[derive(Clone, Debug)]
pub struct OrderByInfo {
    pub column: String,
    pub ascending: bool,
}

/// Extract information from SQL AST
pub fn extract_query_info(statement: &Statement) -> Result<ParsedQuery> {
    match statement {
        Statement::Query(query) => {
            if let SetExpr::Select(select) = &*query.body {
                let tables = extract_tables(select)?;
                let columns = extract_columns(select)?;
                let joins = extract_joins(select)?;
                let filters = extract_filters(select)?;
                let aggregates = extract_aggregates(select)?;
                let group_by = extract_group_by(select)?;
                let order_by = extract_order_by(query)?;
                let (limit, offset) = extract_limit_offset(query)?;
                
                Ok(ParsedQuery {
                    tables,
                    columns,
                    joins,
                    filters,
                    aggregates,
                    window_functions: vec![], // TODO: Extract window functions
                    group_by,
                    having: None, // TODO: Extract HAVING clause
                    order_by,
                    limit,
                    offset,
                    projection_expressions: vec![], // Old parser doesn't extract expressions
                    distinct: false, // TODO: Extract DISTINCT from SELECT
                    table_aliases: std::collections::HashMap::new(), // Old parser doesn't extract aliases
                })
            } else {
                anyhow::bail!("Only SELECT queries are supported")
            }
        }
        _ => anyhow::bail!("Only SELECT queries are supported"),
    }
}

fn extract_tables(select: &Select) -> Result<Vec<String>> {
    let mut tables = vec![];
    
    // Extract from FROM clause
    for item in &select.from {
        match &item.relation {
            TableFactor::Table { name, .. } => {
                // ObjectName is a Vec<Ident>, join them with dots and handle quotes
                let table_name = name.0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                tables.push(table_name);
            }
            _ => {}
        }
    }
    
    // Extract from JOINs
    for item in &select.from {
        for join in &item.joins {
            match &join.relation {
                TableFactor::Table { name, .. } => {
                    // ObjectName is a Vec<Ident>, join them with dots
                    let table_name = name.0.iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<_>>()
                        .join(".");
                    tables.push(table_name);
                }
                _ => {}
            }
        }
    }
    
    Ok(tables)
}

fn extract_columns(select: &Select) -> Result<Vec<String>> {
    let mut columns = vec![];
    
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                // TODO: Extract column name from expression
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(alias.to_string());
            }
            sqlparser::ast::SelectItem::Wildcard(_) => {
                // TODO: Expand wildcard
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                // TODO: Expand qualified wildcard
            }
        }
    }
    
    Ok(columns)
}

fn extract_joins(select: &Select) -> Result<Vec<JoinInfo>> {
    let mut joins = vec![];
    
    for item in &select.from {
        for join in &item.joins {
            // TODO: Extract join information
        }
    }
    
    Ok(joins)
}

fn extract_filters(select: &Select) -> Result<Vec<FilterInfo>> {
    let mut filters = vec![];
    
    if let Some(where_clause) = &select.selection {
        // TODO: Extract filter predicates from WHERE clause
    }
    
    Ok(filters)
}

fn extract_aggregates(select: &Select) -> Result<Vec<AggregateInfo>> {
    let mut aggregates = vec![];
    
    for item in &select.projection {
        // TODO: Extract aggregate functions
    }
    
    Ok(aggregates)
}

fn extract_group_by(select: &Select) -> Result<Vec<String>> {
    let mut group_by = vec![];
    
    // select.group_by is GroupByExpr which contains Vec<Expr>
    match &select.group_by {
        sqlparser::ast::GroupByExpr::All => {
            // GROUP BY ALL - return empty for now
        }
        sqlparser::ast::GroupByExpr::Expressions(exprs) => {
            for expr in exprs {
                // TODO: Extract group by columns from expression
                // For now, just collect as string representation
                group_by.push(format!("{:?}", expr));
            }
        }
    }
    
    Ok(group_by)
}

fn extract_order_by(query: &Query) -> Result<Vec<OrderByInfo>> {
    let mut order_by = vec![];
    
    // query.order_by is Vec<OrderByExpr> (not Option)
    for item in &query.order_by {
        // TODO: Extract order by information from OrderByExpr
        // For now, create placeholder
        order_by.push(OrderByInfo {
            column: format!("{:?}", item.expr),
            ascending: item.asc.unwrap_or(true),
        });
    }
    
    Ok(order_by)
}

fn extract_limit_offset(query: &Query) -> Result<(Option<usize>, Option<usize>)> {
    if let Some(limit) = &query.limit {
        // TODO: Extract limit and offset
    }
    
    Ok((None, None))
}

