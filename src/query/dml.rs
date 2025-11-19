/// Data Manipulation Language (DML) Operations
use sqlparser::ast::*;
use crate::storage::fragment::Value;
use anyhow::Result;

/// Parsed INSERT statement
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
}

/// Parsed UPDATE statement
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<(String, Value)>,
    pub where_clause: Option<Expr>,
}

/// Parsed DELETE statement
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

/// Extract INSERT statement from AST
pub fn extract_insert(statement: &Statement) -> Result<InsertStatement> {
    match statement {
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            let table = table_name.0.iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            
            let column_names: Vec<String> = columns.iter()
                .map(|col| col.value.clone())
                .collect();
            
            // Extract values from VALUES clause
            let mut values = vec![];
            if let Some(query) = source.as_ref() {
                if let SetExpr::Values(values_list) = query.body.as_ref() {
                    for row in &values_list.rows {
                        let mut row_values = vec![];
                        for expr in row {
                            row_values.push(extract_value_from_expr(expr)?);
                        }
                        values.push(row_values);
                    }
                }
            }
            
            Ok(InsertStatement {
                table_name: table,
                columns: column_names,
                values,
            })
        }
        _ => anyhow::bail!("Not an INSERT statement"),
    }
}

/// Extract UPDATE statement from AST
pub fn extract_update(statement: &Statement) -> Result<UpdateStatement> {
    match statement {
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            let table_name = match table {
                TableWithJoins { relation, .. } => {
                    match relation {
                        TableFactor::Table { name, .. } => {
                            name.0.iter().map(|ident| ident.value.clone()).collect::<Vec<_>>().join(".")
                        }
                        _ => anyhow::bail!("UPDATE only supports simple table names"),
                    }
                }
            };
            
            let mut assignment_list = vec![];
            for assignment in assignments {
                match assignment {
                    Assignment { id, value, .. } => {
                        let column = id.iter().map(|ident| ident.value.clone()).collect::<Vec<_>>().join(".");
                        let val = extract_value_from_expr(value)?;
                        assignment_list.push((column, val));
                    }
                }
            }
            
            Ok(UpdateStatement {
                table_name,
                assignments: assignment_list,
                where_clause: selection.clone(),
            })
        }
        _ => anyhow::bail!("Not an UPDATE statement"),
    }
}

/// Extract DELETE statement from AST
pub fn extract_delete(statement: &Statement) -> Result<DeleteStatement> {
    match statement {
        Statement::Delete {
            from,
            selection,
            ..
        } => {
            let table_name = match from.first() {
                Some(TableWithJoins { relation, .. }) => {
                    match relation {
                        TableFactor::Table { name, .. } => {
                            name.0.iter().map(|ident| ident.value.clone()).collect::<Vec<_>>().join(".")
                        }
                        _ => anyhow::bail!("DELETE only supports simple table names"),
                    }
                }
                None => anyhow::bail!("DELETE requires a table"),
            };
            
            Ok(DeleteStatement {
                table_name,
                where_clause: selection.clone(),
            })
        }
        _ => anyhow::bail!("Not a DELETE statement"),
    }
}

/// Extract value from SQL expression
fn extract_value_from_expr(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(val) => match val {
            sqlparser::ast::Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Float64(n.parse()?))
                } else {
                    Ok(Value::Int64(n.parse()?))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) | sqlparser::ast::Value::DoubleQuotedString(s) => {
                Ok(Value::String(s.clone()))
            }
            sqlparser::ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
            sqlparser::ast::Value::Null => Ok(Value::Null),
            _ => anyhow::bail!("Unsupported literal value"),
        }
        _ => anyhow::bail!("Only literal values supported in INSERT/UPDATE"),
    }
}

