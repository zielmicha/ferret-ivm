//! Table functions evaluated per input row (linear, hence trivially
//! incremental).

use crate::error::{codes, Result, SqlError};
use crate::expr::Expr;
use crate::sql::plan::{SrfCall, TableFunc};
use crate::types::{ArrayValue, Datum, Row, SqlType};
use pgfuncs::json::Jsonb;
use pgnumeric::Numeric;

const MAX_SERIES: usize = 100_000_000;

/// The rows `funcs` produce for `row` (several are zipped, the shorter
/// results padded with NULLs); with `ordinality`, each is numbered from 1 in
/// an extra last column.
pub fn eval(funcs: &[SrfCall], row: &[Datum], ordinality: bool) -> Result<Vec<Row>> {
    let mut rows = match funcs {
        [f] => eval_rows(f.func, &f.args, row)?,
        _ => {
            let results: Vec<Vec<Row>> = funcs.iter().map(|f| eval_rows(f.func, &f.args, row)).collect::<Result<_>>()?;
            let n = results.iter().map(|r| r.len()).max().unwrap_or(0);
            (0..n)
                .map(|j| {
                    let mut r = Vec::new();
                    for (f, res) in funcs.iter().zip(&results) {
                        match res.get(j) {
                            Some(x) => r.extend(x.iter().cloned()),
                            None => r.extend(std::iter::repeat_n(Datum::Null, f.arity)),
                        }
                    }
                    r
                })
                .collect()
        }
    };
    if ordinality {
        for (i, r) in rows.iter_mut().enumerate() {
            r.push(Datum::Int8(i as i64 + 1));
        }
    }
    Ok(rows)
}

fn eval_rows(func: TableFunc, args: &[Expr], row: &[Datum]) -> Result<Vec<Row>> {
    let vals: Vec<Datum> = args.iter().map(|a| a.eval(row)).collect::<Result<_>>()?;
    if vals.iter().any(|v| v.is_null()) && !matches!(func, TableFunc::Unnest) {
        return Ok(vec![]);
    }
    Ok(match func {
        TableFunc::GenerateSeries => generate_series(&vals)?,
        TableFunc::Unnest => match &vals[0] {
            Datum::Array(a) => a.elems.iter().map(|e| vec![e.clone()]).collect(),
            _ => vec![],
        },
        TableFunc::JsonbArrayElements | TableFunc::JsonbArrayElementsText => {
            let j = jsonb_arg(&vals[0])?;
            let elems = j.array_elements()?;
            elems
                .into_iter()
                .map(|e| {
                    if func == TableFunc::JsonbArrayElements {
                        vec![Datum::jsonb(e)]
                    } else {
                        vec![e.as_text().map(Datum::text_owned).unwrap_or(Datum::Null)]
                    }
                })
                .collect()
        }
        TableFunc::JsonbEach | TableFunc::JsonbEachText => {
            let j = jsonb_arg(&vals[0])?;
            j.object_entries()?
                .into_iter()
                .map(|(k, v)| {
                    let val = if func == TableFunc::JsonbEach {
                        Datum::jsonb(v)
                    } else {
                        v.as_text().map(Datum::text_owned).unwrap_or(Datum::Null)
                    };
                    vec![Datum::text_owned(k), val]
                })
                .collect()
        }
        TableFunc::JsonbObjectKeys => jsonb_arg(&vals[0])?.object_keys()?.into_iter().map(|k| vec![Datum::text_owned(k)]).collect(),
        TableFunc::JsonbPathQuery { tz } => {
            let path = crate::expr::jsonpath(&vals[1])?;
            let (doc, vars) = (jsonb_arg(&vals[0])?, jsonb_arg(&vals[2])?);
            let silent = vals[3].as_bool() == Some(true);
            crate::expr::path_context(tz, |c| c.query(doc, &path, vars, silent))?
                .into_iter()
                .map(|j| vec![Datum::jsonb(j)])
                .collect()
        }
        TableFunc::RegexpSplitToTable => {
            let flags = vals.get(2).and_then(|d| d.as_str()).unwrap_or("");
            let re = crate::expr::regex_cached(vals[1].as_str().unwrap(), flags)?;
            re.split(vals[0].as_str().unwrap()).into_iter().map(|p| vec![Datum::text_owned(p)]).collect()
        }
        TableFunc::RegexpMatches => {
            let flags = vals.get(2).and_then(|d| d.as_str()).unwrap_or("");
            let global = flags.contains('g');
            let fl: String = flags.chars().filter(|c| *c != 'g').collect();
            let re = crate::expr::regex_cached(vals[1].as_str().unwrap(), &fl)?;
            let s = vals[0].as_str().unwrap();
            let matches = if global { re.all_matches(s) } else { re.match_groups(s).into_iter().collect() };
            matches
                .into_iter()
                .map(|m| {
                    vec![Datum::array(ArrayValue::new_1d(
                        SqlType::Text,
                        m.into_iter().map(|e| e.map(Datum::text_owned).unwrap_or(Datum::Null)).collect(),
                    ))]
                })
                .collect()
        }
    })
}

fn jsonb_arg(d: &Datum) -> Result<&Jsonb> {
    match d {
        Datum::Jsonb(j) => Ok(j),
        _ => Err(SqlError::internal("jsonb argument expected")),
    }
}

fn generate_series(v: &[Datum]) -> Result<Vec<Row>> {
    let mut out = Vec::new();
    match &v[0] {
        Datum::Int4(_) | Datum::Int8(_) => {
            let (a, b) = (v[0].as_i64().unwrap(), v[1].as_i64().unwrap());
            let step = v.get(2).and_then(|d| d.as_i64()).unwrap_or(1);
            if step == 0 {
                return Err(SqlError::new(codes::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
            }
            let int4 = matches!(v[0], Datum::Int4(_));
            let mut x = a;
            while (step > 0 && x <= b) || (step < 0 && x >= b) {
                out.push(vec![if int4 { Datum::Int4(x as i32) } else { Datum::Int8(x) }]);
                if out.len() > MAX_SERIES {
                    return Err(SqlError::new(codes::PROGRAM_LIMIT_EXCEEDED, "generate_series result too large"));
                }
                match x.checked_add(step) {
                    Some(n) => x = n,
                    None => break,
                }
            }
        }
        Datum::Numeric(a) => {
            let b = v[1].as_numeric().unwrap();
            let step = match v.get(2) {
                Some(d) => d.as_numeric().unwrap().clone(),
                None => Numeric::from_i64(1),
            };
            if step.is_zero() {
                return Err(SqlError::new(codes::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
            }
            let neg = step.is_negative();
            let mut x = (**a).clone();
            while (!neg && x <= *b) || (neg && x >= *b) {
                out.push(vec![Datum::numeric(x.clone())]);
                if out.len() > MAX_SERIES {
                    return Err(SqlError::new(codes::PROGRAM_LIMIT_EXCEEDED, "generate_series result too large"));
                }
                x = x.add(&step)?;
            }
        }
        Datum::Timestamp(_) | Datum::TimestampTz(_) => {
            let tz = matches!(v[0], Datum::TimestampTz(_));
            let (Datum::Timestamp(a) | Datum::TimestampTz(a)) = v[0] else { unreachable!() };
            let (Datum::Timestamp(b) | Datum::TimestampTz(b)) = v[1] else {
                return Err(SqlError::internal("generate_series bounds"));
            };
            let step = v.get(2).and_then(|d| d.as_interval()).ok_or_else(|| SqlError::internal("generate_series step"))?;
            if step.months == 0 && step.days == 0 && step.micros == 0 {
                return Err(SqlError::new(codes::INVALID_PARAMETER_VALUE, "step size cannot equal zero"));
            }
            let forward = pgdatetime::interval_cmp_value(step) > 0;
            let mut x = a;
            while (forward && x <= b) || (!forward && x >= b) {
                out.push(vec![if tz { Datum::TimestampTz(x) } else { Datum::Timestamp(x) }]);
                if out.len() > MAX_SERIES {
                    return Err(SqlError::new(codes::PROGRAM_LIMIT_EXCEEDED, "generate_series result too large"));
                }
                x = if tz {
                    pgdatetime::timestamptz_pl_interval(x, step, &crate::types::text::SESSION_TZ)?
                } else {
                    pgdatetime::timestamp_pl_interval(x, step)?
                };
            }
        }
        _ => return Err(SqlError::internal("generate_series argument types")),
    }
    Ok(out)
}
