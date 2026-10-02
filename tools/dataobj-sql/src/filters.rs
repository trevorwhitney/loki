//! Translation of DataFusion filter expressions into scan predicates.
//!
//! Only simple shapes are translated: `column <op> literal` (either order),
//! and `column IN (literal, ...)`. Anything else is left for DataFusion to
//! evaluate. Because the server treats predicates as a pruning hint, every
//! translated filter is reported as `Inexact` so DataFusion re-applies it.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

use crate::scanpb::{literal, Literal, Op, Predicate};

/// Converts a filter expression into a scan predicate, if it has a shape the
/// server understands.
pub fn to_predicate(expr: &Expr) -> Option<Predicate> {
    match expr {
        // DataFusion rewrites short IN lists into OR chains of equalities
        // before filters reach the provider; fold them back into one IN.
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Or, ..
        }) => {
            let mut leaves = Vec::new();
            collect_or_leaves(expr, &mut leaves);
            let mut column: Option<&str> = None;
            let mut values = Vec::with_capacity(leaves.len());
            for leaf in leaves {
                let Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }) = leaf
                else {
                    return None;
                };
                let (c, v) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v, _))
                    | (Expr::Literal(v, _), Expr::Column(c)) => (c, v),
                    _ => return None,
                };
                match column {
                    None => column = Some(c.name.as_str()),
                    Some(name) if name == c.name => {}
                    Some(_) => return None,
                }
                values.push(to_literal(v)?);
            }
            Some(Predicate {
                column: column?.to_string(),
                op: Op::In as i32,
                values,
            })
        }

        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (column, value, op) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(v, _)) => (c, v, *op),
                (Expr::Literal(v, _), Expr::Column(c)) => (c, v, op.swap()?),
                _ => return None,
            };
            let op = match op {
                Operator::Eq => Op::Eq,
                Operator::Gt => Op::Gt,
                Operator::GtEq => Op::Gte,
                Operator::Lt => Op::Lt,
                Operator::LtEq => Op::Lte,
                _ => return None,
            };
            Some(Predicate {
                column: column.name.clone(),
                op: op as i32,
                values: vec![to_literal(value)?],
            })
        }

        Expr::InList(in_list) if !in_list.negated => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return None;
            };
            let values = in_list
                .list
                .iter()
                .map(|e| match e {
                    Expr::Literal(v, _) => to_literal(v),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            if values.is_empty() {
                return None;
            }
            Some(Predicate {
                column: column.name.clone(),
                op: Op::In as i32,
                values,
            })
        }

        _ => None,
    }
}

fn to_literal(value: &ScalarValue) -> Option<Literal> {
    let v = match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => literal::Value::StringValue(s.clone()),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => literal::Value::BytesValue(b.clone()),
        ScalarValue::Int8(Some(i)) => literal::Value::Int64Value(i64::from(*i)),
        ScalarValue::Int16(Some(i)) => literal::Value::Int64Value(i64::from(*i)),
        ScalarValue::Int32(Some(i)) => literal::Value::Int64Value(i64::from(*i)),
        ScalarValue::Int64(Some(i)) => literal::Value::Int64Value(*i),
        ScalarValue::UInt8(Some(u)) => literal::Value::Uint64Value(u64::from(*u)),
        ScalarValue::UInt16(Some(u)) => literal::Value::Uint64Value(u64::from(*u)),
        ScalarValue::UInt32(Some(u)) => literal::Value::Uint64Value(u64::from(*u)),
        ScalarValue::UInt64(Some(u)) => literal::Value::Uint64Value(*u),
        ScalarValue::TimestampSecond(Some(t), _) => {
            literal::Value::TimestampNs(t.checked_mul(1_000_000_000)?)
        }
        ScalarValue::TimestampMillisecond(Some(t), _) => {
            literal::Value::TimestampNs(t.checked_mul(1_000_000)?)
        }
        ScalarValue::TimestampMicrosecond(Some(t), _) => {
            literal::Value::TimestampNs(t.checked_mul(1_000)?)
        }
        ScalarValue::TimestampNanosecond(Some(t), _) => literal::Value::TimestampNs(*t),
        _ => return None,
    };
    Some(Literal { value: Some(v) })
}

fn collect_or_leaves<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            collect_or_leaves(left, out);
            collect_or_leaves(right, out);
        }
        other => out.push(other),
    }
}
