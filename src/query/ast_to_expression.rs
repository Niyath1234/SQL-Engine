/// Convert SQL AST expressions to our Expression enum
use sqlparser::ast::*;
use crate::query::expression::{Expression, BinaryOperator, UnaryOperator};
use crate::storage::fragment::Value;
use anyhow::Result;

pub fn sql_expr_to_expression(expr: &Expr) -> Result<Expression> {
    match expr {
        Expr::Identifier(ident) => {
            Ok(Expression::Column(ident.value.clone(), None))
        }
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok(Expression::Column(idents[1].value.clone(), Some(idents[0].value.clone())))
            } else {
                let col_name = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                Ok(Expression::Column(col_name, None))
            }
        }
        Expr::Value(val) => {
            use crate::storage::fragment::Value as FragmentValue;
            let value = match val {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        FragmentValue::Int64(i)
                    } else if let Ok(f) = n.parse::<f64>() {
                        FragmentValue::Float64(f)
                    } else {
                        FragmentValue::String(n.clone())
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s) | sqlparser::ast::Value::DoubleQuotedString(s) => FragmentValue::String(s.clone()),
                sqlparser::ast::Value::Boolean(b) => FragmentValue::Int64(if *b { 1 } else { 0 }),
                sqlparser::ast::Value::Null => FragmentValue::Null,
                _ => FragmentValue::String(format!("{:?}", val)),
            };
            Ok(Expression::Literal(value))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_expr = sql_expr_to_expression(left)?;
            let right_expr = sql_expr_to_expression(right)?;
            use sqlparser::ast::BinaryOperator as SqlBinaryOp;
            use crate::query::expression::BinaryOperator as ExprBinaryOp;
            let bin_op = match op {
                SqlBinaryOp::Plus => ExprBinaryOp::Add,
                SqlBinaryOp::Minus => ExprBinaryOp::Subtract,
                SqlBinaryOp::Multiply => ExprBinaryOp::Multiply,
                SqlBinaryOp::Divide => ExprBinaryOp::Divide,
                SqlBinaryOp::Modulo => ExprBinaryOp::Modulo,
                SqlBinaryOp::Gt => ExprBinaryOp::Gt,
                SqlBinaryOp::GtEq => ExprBinaryOp::Ge,
                SqlBinaryOp::Lt => ExprBinaryOp::Lt,
                SqlBinaryOp::LtEq => ExprBinaryOp::Le,
                SqlBinaryOp::Eq => ExprBinaryOp::Eq,
                SqlBinaryOp::NotEq => ExprBinaryOp::Ne,
                SqlBinaryOp::And => ExprBinaryOp::And,
                SqlBinaryOp::Or => ExprBinaryOp::Or,
                SqlBinaryOp::StringConcat => ExprBinaryOp::Concat,
                _ => anyhow::bail!("Unsupported binary operator: {:?}", op),
            };
            Ok(Expression::BinaryOp {
                left: Box::new(left_expr),
                op: bin_op,
                right: Box::new(right_expr),
            })
        }
        Expr::UnaryOp { op, expr: inner } => {
            let inner_expr = sql_expr_to_expression(inner)?;
            use sqlparser::ast::UnaryOperator as SqlUnaryOp;
            use crate::query::expression::UnaryOperator as ExprUnaryOp;
            let unary_op = match op {
                SqlUnaryOp::Plus => ExprUnaryOp::Plus,
                SqlUnaryOp::Minus => ExprUnaryOp::Negate,
                SqlUnaryOp::Not => ExprUnaryOp::Not,
                _ => anyhow::bail!("Unsupported unary operator: {:?}", op),
            };
            Ok(Expression::UnaryOp {
                op: unary_op,
                expr: Box::new(inner_expr),
            })
        }
        Expr::Case { operand, conditions, results, else_result } => {
            let operand_expr = operand.as_ref().map(|e| sql_expr_to_expression(e)).transpose()?;
            let mut condition_pairs = Vec::new();
            
            // SQL parser stores conditions and results separately, but our Expression::Case expects pairs
            // For CASE WHEN (no operand), conditions are boolean expressions
            // For CASE expr WHEN (with operand), conditions are values to compare
            for (condition, result) in conditions.iter().zip(results.iter()) {
                let cond_expr = sql_expr_to_expression(condition)?;
                let result_expr = sql_expr_to_expression(result)?;
                condition_pairs.push((cond_expr, result_expr));
            }
            
            let else_expr = else_result.as_ref().map(|e| sql_expr_to_expression(e)).transpose()?;
            
            Ok(Expression::Case {
                operand: operand_expr.map(Box::new),
                conditions: condition_pairs,
                else_result: else_expr.map(Box::new),
            })
        }
        Expr::Function(func) => {
            // Convert function arguments
            let mut args = Vec::new();
            for arg in &func.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        args.push(sql_expr_to_expression(expr)?);
                    }
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                        // COUNT(*) - represent as a special literal
                        args.push(Expression::Literal(Value::String("*".to_string())));
                    }
                    FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
                        // COUNT(table.*) - represent as a special literal
                        args.push(Expression::Literal(Value::String("*".to_string())));
                    }
                    FunctionArg::Named { name: _, arg: FunctionArgExpr::Expr(expr) } => {
                        args.push(sql_expr_to_expression(expr)?);
                    }
                    FunctionArg::Named { name: _, arg: FunctionArgExpr::Wildcard } => {
                        // Named wildcard argument
                        args.push(Expression::Literal(Value::String("*".to_string())));
                    }
                    FunctionArg::Named { name: _, arg: FunctionArgExpr::QualifiedWildcard(_) } => {
                        // Named qualified wildcard argument
                        args.push(Expression::Literal(Value::String("*".to_string())));
                    }
                }
            }
            
            Ok(Expression::Function {
                name: func.name.to_string(),
                args,
            })
        }
        Expr::Subquery(query) => {
            // Convert subquery to Expression::Subquery
            Ok(Expression::Subquery(query.clone()))
        }
        Expr::Exists { subquery, negated } => {
            // Convert EXISTS subquery
            let subquery_expr = Expression::Exists(subquery.clone());
            if *negated {
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(subquery_expr),
                })
            } else {
                Ok(subquery_expr)
            }
        }
        Expr::InSubquery { expr: _, subquery, negated } => {
            // IN subquery - for now, convert to EXISTS for simplicity
            // TODO: Properly implement IN subquery
            let exists_expr = Expression::Exists(subquery.clone());
            if *negated {
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(exists_expr),
                })
            } else {
                Ok(exists_expr)
            }
        }
        _ => anyhow::bail!("Unsupported expression type: {:?}", expr)
    }
}

