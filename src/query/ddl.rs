/// Data Definition Language (DDL) Operations
use sqlparser::ast::*;
use arrow::datatypes::DataType as ArrowDataType;
use anyhow::Result;
use crate::storage::fragment::Value as FragmentValue;

/// Parsed CREATE TABLE statement
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
}

/// Column definition from CREATE TABLE
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: ArrowDataType,
    pub nullable: bool,
    pub default: Option<FragmentValue>,
}

/// Extract CREATE TABLE statement from AST
pub fn extract_create_table(statement: &Statement) -> Result<CreateTableStatement> {
    match statement {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            let table_name = name.0.iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            
            let mut column_defs = vec![];
            for col in columns {
                let col_name = col.name.value.clone();
                
                // Convert SQL type to Arrow type
                let data_type = sql_type_to_arrow_type(&col.data_type)?;
                
                let nullable = !col.options.iter().any(|opt| {
                    matches!(opt.option, ColumnOption::NotNull { .. })
                });
                let default = col.options.iter()
                    .find_map(|opt| {
                        if let ColumnOption::Default(expr) = &opt.option {
                            extract_default_value(expr).ok()
                        } else {
                            None
                        }
                    });
                
                column_defs.push(ColumnDefinition {
                    name: col_name,
                    data_type,
                    nullable,
                    default,
                });
            }
            
            Ok(CreateTableStatement {
                table_name,
                columns: column_defs,
                constraints: constraints.clone(),
            })
        }
        _ => anyhow::bail!("Not a CREATE TABLE statement"),
    }
}

/// Extract DROP TABLE statement
pub fn extract_drop_table(statement: &Statement) -> Result<String> {
    match statement {
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            if matches!(object_type, ObjectType::Table) {
                let table_name = names.first()
                    .ok_or_else(|| anyhow::anyhow!("DROP TABLE requires a table name"))?
                    .0.iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                Ok(table_name)
            } else {
                anyhow::bail!("Only DROP TABLE is supported")
            }
        }
        _ => anyhow::bail!("Not a DROP statement"),
    }
}

/// Convert SQL type to Arrow type
fn sql_type_to_arrow_type(sql_type: &sqlparser::ast::DataType) -> Result<ArrowDataType> {
    match sql_type {
        sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => Ok(ArrowDataType::Int64),
        sqlparser::ast::DataType::BigInt(_) => Ok(ArrowDataType::Int64),
        sqlparser::ast::DataType::SmallInt(_) => Ok(ArrowDataType::Int64),
        sqlparser::ast::DataType::TinyInt(_) => Ok(ArrowDataType::Int64),
        sqlparser::ast::DataType::Float(_) => Ok(ArrowDataType::Float64),
        sqlparser::ast::DataType::Double => Ok(ArrowDataType::Float64),
        sqlparser::ast::DataType::Real => Ok(ArrowDataType::Float64),
        sqlparser::ast::DataType::Char(_) | sqlparser::ast::DataType::Varchar(_) | sqlparser::ast::DataType::Text => Ok(ArrowDataType::Utf8),
        sqlparser::ast::DataType::Boolean => Ok(ArrowDataType::Boolean),
        sqlparser::ast::DataType::Timestamp(_, _) => {
            // Store timestamps as Int64 (milliseconds since epoch)
            Ok(ArrowDataType::Int64)
        },
        sqlparser::ast::DataType::Custom(name, _) => {
            // Handle custom types like UUID, STRING
            let type_name = name.0.iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            if type_name.eq_ignore_ascii_case("uuid") || type_name.eq_ignore_ascii_case("string") {
                Ok(ArrowDataType::Utf8) // Store UUIDs and STRING as strings
            } else {
                anyhow::bail!("Unsupported custom data type: {}", type_name)
            }
        },
        _ => anyhow::bail!("Unsupported data type: {:?}", sql_type),
    }
}

/// Extract default value from expression
fn extract_default_value(expr: &Expr) -> Result<FragmentValue> {
    match expr {
        Expr::Value(val) => match val {
            sqlparser::ast::Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(FragmentValue::Float64(n.parse()?))
                } else {
                    Ok(FragmentValue::Int64(n.parse()?))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) | sqlparser::ast::Value::DoubleQuotedString(s) => {
                Ok(FragmentValue::String(s.clone()))
            }
            sqlparser::ast::Value::Boolean(b) => Ok(FragmentValue::Bool(*b)),
            sqlparser::ast::Value::Null => Ok(FragmentValue::Null),
            _ => anyhow::bail!("Unsupported default value"),
        }
        Expr::Function(func) => {
            // Handle function calls like NOW(), CURRENT_TIMESTAMP, etc.
            let func_name = func.name.to_string().to_uppercase();
            match func_name.as_str() {
                "NOW" | "CURRENT_TIMESTAMP" | "CURRENT_TIMESTAMP()" => {
                    // Return current timestamp in milliseconds since epoch
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    Ok(FragmentValue::Int64(now_ms))
                }
                "CURRENT_DATE" | "CURRENT_DATE()" => {
                    // Return current date as string (YYYY-MM-DD)
                    // Use system time and format manually
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    // Simple date formatting (approximate)
                    let days = now / 86400;
                    let epoch_days = days as i64 - 719163; // Days since 1970-01-01
                    // Approximate date calculation
                    let year = 1970 + (epoch_days / 365);
                    let day_of_year = epoch_days % 365;
                    let month = (day_of_year / 30) + 1;
                    let day = (day_of_year % 30) + 1;
                    let date_str = format!("{:04}-{:02}-{:02}", year, month, day);
                    Ok(FragmentValue::String(date_str))
                }
                "CURRENT_TIME" | "CURRENT_TIME()" => {
                    // Return current time as string (HH:MM:SS)
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let seconds_in_day = now % 86400;
                    let hours = seconds_in_day / 3600;
                    let minutes = (seconds_in_day % 3600) / 60;
                    let secs = seconds_in_day % 60;
                    let time_str = format!("{:02}:{:02}:{:02}", hours, minutes, secs);
                    Ok(FragmentValue::String(time_str))
                }
                _ => anyhow::bail!("Unsupported default function: {}", func_name),
            }
        }
        _ => anyhow::bail!("Only literal default values or NOW()/CURRENT_TIMESTAMP supported"),
    }
}

