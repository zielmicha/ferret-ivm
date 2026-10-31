//! jsonpath execution: a port of src/backend/utils/adt/jsonpath_exec.c of
//! PostgreSQL 17 (Copyright (c) 2019-2024, PostgreSQL Global Development
//! Group; PostgreSQL License).
//!
//! The structure of the C executor is kept function by function: the same
//! evaluation order, the same short cuts when no result list is requested
//! (existence checks stop at the first item, which decides which errors are
//! raised), lax/strict unwrapping and wrapping, three-valued predicates and
//! the error suppression rules of `silent` mode.
//!
//! PostgreSQL's `check_stack_depth()` calls are emulated: every executor
//! function adds its C stack frame size (measured on PostgreSQL 17, x86-64)
//! to a running total, honouring the calls GCC compiles as tail calls, and
//! the checks fail at PostgreSQL's default 2MB `max_stack_depth`.

use super::ast::*;
use super::datetime::{self, DateTime};
use super::{exec_error, layout};
use crate::json::{JsonNumber, Jsonb, find_key};
use crate::{Error, Result, sqlstate};
use pgnumeric::Numeric;
use std::borrow::Cow;
use std::rc::Rc;

/// JsonPathExecResult
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Jper {
    Ok,
    NotFound,
    Error,
}

/// JsonPathBool
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JpBool {
    False,
    True,
    Unknown,
}

/// An SQL/JSON item (JsonbValue).
#[derive(Clone, Debug)]
pub(super) enum Val<'a> {
    Null,
    Bool(bool),
    Num(Cow<'a, JsonNumber>),
    Str(Cow<'a, str>),
    /// array or object of the document or of `vars` (jbvBinary)
    Container(&'a Jsonb),
    /// object `{"id": .., "key": .., "value": ..}` built by `.keyvalue()`
    Kv(Rc<KvObj<'a>>),
    DateTime(DateTime),
}

#[derive(Debug)]
pub(super) struct KvObj<'a> {
    pub key: Cow<'a, str>,
    pub value: Val<'a>,
    pub id: i64,
}

/// jbvType of an item, as JsonbType() reports it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JType {
    Null,
    Str,
    Num,
    Bool,
    Array,
    Object,
    DateTime,
}

impl<'a> Val<'a> {
    /// A jsonb value as an SQL/JSON item: scalars are extracted, containers
    /// are referenced.
    pub fn from_jsonb(v: &'a Jsonb) -> Val<'a> {
        match v {
            Jsonb::Null => Val::Null,
            Jsonb::Bool(b) => Val::Bool(*b),
            Jsonb::Number(n) => Val::Num(Cow::Borrowed(n)),
            Jsonb::String(s) => Val::Str(Cow::Borrowed(s.as_str())),
            Jsonb::Array(_) | Jsonb::Object(_) => Val::Container(v),
        }
    }

    fn jtype(&self) -> JType {
        match self {
            Val::Null => JType::Null,
            Val::Bool(_) => JType::Bool,
            Val::Num(_) => JType::Num,
            Val::Str(_) => JType::Str,
            Val::Container(Jsonb::Array(_)) => JType::Array,
            Val::Container(_) | Val::Kv(_) => JType::Object,
            Val::DateTime(_) => JType::DateTime,
        }
    }

    /// The raw JsonbValue type used by compareItems(): arrays and objects
    /// are both jbvBinary.
    fn raw_type(&self) -> u8 {
        match self {
            Val::Null => 0,
            Val::Str(_) => 1,
            Val::Num(_) => 2,
            Val::Bool(_) => 3,
            Val::Container(_) | Val::Kv(_) => 4,
            Val::DateTime(_) => 5,
        }
    }

    fn num(&self) -> Option<&JsonNumber> {
        match self {
            Val::Num(n) => Some(n),
            _ => None,
        }
    }

    fn str(&self) -> Option<&str> {
        match self {
            Val::Str(s) => Some(s),
            _ => None,
        }
    }

    /// JsonbArraySize(): -1 if not an array.
    fn array_size(&self) -> i32 {
        match self {
            Val::Container(Jsonb::Array(a)) => a.len() as i32,
            _ => -1,
        }
    }

    /// JsonbTypeName()
    fn type_name(&self) -> &'static str {
        match self {
            Val::Null => "null",
            Val::Bool(_) => "boolean",
            Val::Num(_) => "number",
            Val::Str(_) => "string",
            Val::Container(Jsonb::Array(_)) => "array",
            Val::Container(_) | Val::Kv(_) => "object",
            Val::DateTime(d) => d.type_name(),
        }
    }

    /// The children of a container (values of an object, in key order), as
    /// JsonbIteratorNext(skipNested = true) returns them.
    fn children(&self) -> Vec<Val<'a>> {
        match self {
            Val::Container(Jsonb::Array(a)) => a.iter().map(Val::from_jsonb).collect(),
            Val::Container(Jsonb::Object(p)) => p.iter().map(|(_, v)| Val::from_jsonb(v)).collect(),
            Val::Kv(kv) => vec![
                Val::Num(Cow::Owned(JsonNumber::from_i64(kv.id))),
                Val::Str(kv.key.clone()),
                kv.value.clone(),
            ],
            _ => Vec::new(),
        }
    }

    /// Key/value pairs of an object, in storage order.
    fn pairs(&self) -> Vec<(Cow<'a, str>, Val<'a>)> {
        match self {
            Val::Container(Jsonb::Object(p)) => p
                .iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Val::from_jsonb(v)))
                .collect(),
            Val::Kv(kv) => vec![
                (
                    Cow::Borrowed("id"),
                    Val::Num(Cow::Owned(JsonNumber::from_i64(kv.id))),
                ),
                (Cow::Borrowed("key"), Val::Str(kv.key.clone())),
                (Cow::Borrowed("value"), kv.value.clone()),
            ],
            _ => Vec::new(),
        }
    }

    /// findJsonbValueFromContainer() for an object key.
    fn get_key(&self, key: &str) -> Option<Val<'a>> {
        match self {
            Val::Container(Jsonb::Object(p)) => find_key(p, key).map(|i| Val::from_jsonb(&p[i].1)),
            Val::Kv(kv) => match key {
                "id" => Some(Val::Num(Cow::Owned(JsonNumber::from_i64(kv.id)))),
                "key" => Some(Val::Str(kv.key.clone())),
                "value" => Some(kv.value.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    fn object_len(&self) -> usize {
        match self {
            Val::Container(Jsonb::Object(p)) => p.len(),
            Val::Kv(_) => 3,
            _ => 0,
        }
    }

    /// JsonbValueToJsonb()
    pub fn to_jsonb(&self) -> Result<Jsonb> {
        Ok(match self {
            Val::Null => Jsonb::Null,
            Val::Bool(b) => Jsonb::Bool(*b),
            Val::Num(n) => Jsonb::Number(n.as_ref().clone()),
            Val::Str(s) => Jsonb::String(s.to_string()),
            Val::Container(c) => (*c).clone(),
            Val::Kv(kv) => Jsonb::Object(vec![
                ("id".to_owned(), Jsonb::Number(JsonNumber::from_i64(kv.id))),
                ("key".to_owned(), Jsonb::String(kv.key.to_string())),
                ("value".to_owned(), kv.value.to_jsonb()?),
            ]),
            Val::DateTime(d) => Jsonb::String(d.to_json_string()?),
        })
    }
}

type List<'a> = Vec<Val<'a>>;

/// The base object of `.keyvalue()` ids (JsonBaseObjectInfo).
#[derive(Clone)]
enum BaseRoot<'a> {
    None,
    Json(&'a Jsonb),
    Kv(Rc<KvObj<'a>>),
}

#[derive(Clone)]
struct Base<'a> {
    root: BaseRoot<'a>,
    id: i32,
}

/// Predicate callbacks of executePredicate().
#[derive(Clone, Copy)]
enum Pred {
    Comparison(BinOp),
    StartsWith,
    LikeRegex,
}

// C stack frame sizes in bytes (see the module docs).
const F_EOUT: u32 = 640; // executeItemOptUnwrapTarget
const F_ENI: u32 = 112; // executeNextItem (with executeItem inlined)
const F_EAI: u32 = 208; // executeAnyItem
const F_EUTA: u32 = 80; // executeItemUnwrapTargetArray
const F_EIOUR: u32 = 112; // executeItemOptUnwrapResult
const F_EBAE: u32 = 176; // executeBinaryArithmExpr
const F_EUAE: u32 = 96; // executeUnaryArithmExpr
const F_EBI: u32 = 160; // executeBoolItem
const F_EP: u32 = 208; // executePredicate
const F_ENBI: u32 = 0; // executeNestedBoolItem
const F_GAI: u32 = 80; // getArrayIndex
const F_EKVM: u32 = 528; // executeKeyValueMethod
const F_ENIM: u32 = 128; // executeNumericItemMethod
const F_EDTM: u32 = 256; // executeDateTimeMethod
const F_NOTHROW: u32 = 0; // executeItemOptUnwrapResultNoThrow
/// max_stack_depth (2MB) minus the stack in use when execution starts.
const STACK_BUDGET: u32 = 2_093_900;

pub(super) struct Exec<'a> {
    nodes: &'a [Node],
    vars: Option<&'a Jsonb>,
    root: Val<'a>,
    current: Val<'a>,
    base: Base<'a>,
    last_generated_object_id: i32,
    innermost_array_size: i32,
    lax: bool,
    ignore_structural_errors: bool,
    throw_errors: bool,
    use_tz: bool,
    stack_used: u32,
    tz: &'a datetime::SessionTz,
    offsets: layout::OffsetCache,
}

macro_rules! return_error {
    ($s:expr, $e:expr) => {{
        if $s.throw_errors {
            return Err($e);
        }
        return Ok(Jper::Error);
    }};
}

fn err(code: &'static str, msg: impl Into<String>) -> Error {
    Error::new(code, msg)
}

/// A finite numeric as a JSON number.
fn from_numeric(n: Numeric) -> JsonNumber {
    JsonNumber::from_numeric(n).expect("finite jsonpath numeric result")
}

fn num_val<'a>(n: JsonNumber) -> Val<'a> {
    Val::Num(Cow::Owned(n))
}

/// countVariablesFromJsonb()
fn count_vars(vars: Option<&Jsonb>) -> Result<i32> {
    match vars {
        None => Ok(0),
        Some(Jsonb::Object(_)) => Ok(1),
        Some(_) => Err(Error::with_detail(
            sqlstate::INVALID_PARAMETER_VALUE,
            "\"vars\" argument is not an object",
            "Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.",
        )),
    }
}

/// executeJsonPath()
#[allow(clippy::too_many_arguments)]
pub(super) fn execute<'a>(
    nodes: &'a [Node],
    root: NodeId,
    lax: bool,
    vars: Option<&'a Jsonb>,
    json: &'a Jsonb,
    throw_errors: bool,
    result: Option<&mut List<'a>>,
    use_tz: bool,
    tz: &'a datetime::SessionTz,
) -> Result<Jper> {
    let jbv = Val::from_jsonb(json);
    let last_generated_object_id = 1 + count_vars(vars)?;
    let mut cx = Exec {
        nodes,
        vars,
        root: jbv.clone(),
        current: jbv.clone(),
        base: Base {
            root: BaseRoot::None,
            id: 0,
        },
        last_generated_object_id,
        innermost_array_size: -1,
        lax,
        ignore_structural_errors: lax,
        throw_errors,
        use_tz,
        stack_used: 0,
        tz,
        offsets: layout::OffsetCache::default(),
    };
    if !lax && result.is_none() {
        // In strict mode we must get a complete list of values to check that
        // there are no errors at all.
        let mut vals = List::new();
        let res = cx.execute_item(root, &jbv, Some(&mut vals))?;
        if res == Jper::Error {
            return Ok(res);
        }
        return Ok(if vals.is_empty() {
            Jper::NotFound
        } else {
            Jper::Ok
        });
    }
    cx.execute_item(root, &jbv, result)
}

impl<'a> Exec<'a> {
    // ------------------------------------------------------ stack emulation

    #[inline]
    fn enter(&mut self, cost: u32) {
        self.stack_used += cost;
    }

    /// check_stack_depth()
    #[inline]
    fn check_stack(&self) -> Result<()> {
        if self.stack_used > STACK_BUDGET {
            return Err(stack_depth_error());
        }
        Ok(())
    }

    #[inline]
    fn leave(&mut self, cost: u32) {
        self.stack_used -= cost;
    }

    /// A call GCC compiles as a tail call: the caller's frame (`cost`) is
    /// released while `f` runs.
    #[inline]
    fn tail<R>(&mut self, cost: u32, f: impl FnOnce(&mut Self) -> R) -> R {
        self.leave(cost);
        let r = f(self);
        self.enter(cost);
        r
    }

    fn node(&self, id: NodeId) -> &'a Node {
        &self.nodes[id as usize]
    }

    // ------------------------------------------------------------ executor

    /// executeItem()
    fn execute_item(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        let unwrap = self.lax;
        self.exec_opt_unwrap_target(jsp, jb, found, unwrap)
    }

    /// executeItemOptUnwrapTarget()
    fn exec_opt_unwrap_target(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
        unwrap: bool,
    ) -> Result<Jper> {
        self.enter(F_EOUT);
        self.check_stack()?;
        let r = self.eout_body(jsp, jb, found, unwrap);
        self.leave(F_EOUT);
        r
    }

    fn eout_body(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        mut found: Option<&mut List<'a>>,
        unwrap: bool,
    ) -> Result<Jper> {
        let node = self.node(jsp);
        let next = node.next;
        let mut res = Jper::NotFound;
        match &node.kind {
            Kind::Null | Kind::Bool(_) | Kind::Numeric(_) | Kind::String(_) | Kind::Variable(_) => {
                if next.is_none() && found.is_none() && !matches!(node.kind, Kind::Variable(_)) {
                    // Skip evaluation, but not for variables: we must
                    // trigger an error for the missing variable.
                    return Ok(Jper::Ok);
                }
                let saved = self.base.clone();
                let v = self.get_jsonpath_item(jsp)?;
                res = self.exec_next(next, v, found)?;
                self.base = saved;
            }

            // all boolean item types
            Kind::Binary(
                BinOp::And
                | BinOp::Or
                | BinOp::Equal
                | BinOp::NotEqual
                | BinOp::Less
                | BinOp::Greater
                | BinOp::LessOrEqual
                | BinOp::GreaterOrEqual
                | BinOp::StartsWith,
                ..,
            )
            | Kind::Not(_)
            | Kind::IsUnknown(_)
            | Kind::Exists(_)
            | Kind::LikeRegex { .. } => {
                let st = self.exec_bool_item(jsp, jb, true)?;
                res = self.tail(F_EOUT, |s| s.append_bool_result(jsp, found, st))?;
            }

            Kind::Binary(op, ..) => {
                let op = *op;
                return self.tail(F_EOUT, |s| s.exec_binary_arithm(jsp, op, jb, found));
            }
            Kind::Plus(_) => {
                return self.tail(F_EOUT, |s| s.exec_unary_arithm(jsp, false, jb, found));
            }
            Kind::Minus(_) => {
                return self.tail(F_EOUT, |s| s.exec_unary_arithm(jsp, true, jb, found));
            }

            Kind::AnyArray => {
                if jb.jtype() == JType::Array {
                    let unwrap_elements = self.lax;
                    res = self.exec_unwrap_target_array(next, jb, found, unwrap_elements)?;
                } else if self.lax {
                    res = self.exec_next(next, jb.clone(), found)?;
                } else if !self.ignore_structural_errors {
                    return_error!(
                        self,
                        err(
                            sqlstate::SQL_JSON_ARRAY_NOT_FOUND,
                            "jsonpath wildcard array accessor can only be applied to an array"
                        )
                    );
                }
            }

            Kind::AnyKey => {
                if jb.jtype() == JType::Object {
                    let unwrap_next = self.lax;
                    return self.exec_any_item(next, jb, found, 1, 1, 1, false, unwrap_next);
                } else if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                } else if !self.ignore_structural_errors {
                    return_error!(
                        self,
                        err(
                            sqlstate::SQL_JSON_OBJECT_NOT_FOUND,
                            "jsonpath wildcard member accessor can only be applied to an object"
                        )
                    );
                }
            }

            Kind::IndexArray(subs) => {
                if jb.jtype() == JType::Array || self.lax {
                    let innermost_array_size = self.innermost_array_size;
                    let mut size = jb.array_size();
                    let singleton = size < 0;
                    if singleton {
                        size = 1;
                    }
                    self.innermost_array_size = size; // for LAST evaluation
                    'subs: for &(from, to) in subs.iter() {
                        let index_from = match self.get_array_index(from, jb)? {
                            Ok(i) => i,
                            Err(r) => {
                                res = r;
                                break 'subs;
                            }
                        };
                        let index_to = match to {
                            Some(to) => match self.get_array_index(to, jb)? {
                                Ok(i) => i,
                                Err(r) => {
                                    res = r;
                                    break 'subs;
                                }
                            },
                            None => index_from,
                        };
                        if !self.ignore_structural_errors
                            && (index_from < 0 || index_from > index_to || index_to >= size)
                        {
                            return_error!(
                                self,
                                err(
                                    sqlstate::INVALID_SQL_JSON_SUBSCRIPT,
                                    "jsonpath array subscript is out of bounds"
                                )
                            );
                        }
                        let index_from = index_from.max(0);
                        let index_to = index_to.min(size - 1);
                        res = Jper::NotFound;
                        let mut index = index_from;
                        while index <= index_to {
                            let v = if singleton {
                                jb.clone()
                            } else {
                                match jb {
                                    Val::Container(Jsonb::Array(a)) => {
                                        match a.get(index as usize) {
                                            Some(e) => Val::from_jsonb(e),
                                            None => {
                                                index += 1;
                                                continue;
                                            }
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            };
                            if next.is_none() && found.is_none() {
                                return Ok(Jper::Ok);
                            }
                            res = self.exec_next(next, v, found.as_deref_mut())?;
                            if res == Jper::Error || (res == Jper::Ok && found.is_none()) {
                                break;
                            }
                            index += 1;
                        }
                        if res == Jper::Error || (res == Jper::Ok && found.is_none()) {
                            break;
                        }
                    }
                    self.innermost_array_size = innermost_array_size;
                } else if !self.ignore_structural_errors {
                    return_error!(
                        self,
                        err(
                            sqlstate::SQL_JSON_ARRAY_NOT_FOUND,
                            "jsonpath array accessor can only be applied to an array"
                        )
                    );
                }
            }

            Kind::Any { first, last } => {
                let (first, last) = (*first, *last);
                // first try without any intermediate steps
                if first == 0 {
                    let saved = self.ignore_structural_errors;
                    self.ignore_structural_errors = true;
                    res = self.exec_next(next, jb.clone(), found.as_deref_mut())?;
                    self.ignore_structural_errors = saved;
                    if res == Jper::Ok && found.is_none() {
                        return Ok(res);
                    }
                }
                if matches!(jb, Val::Container(_) | Val::Kv(_)) {
                    let unwrap_next = self.lax;
                    res = self.exec_any_item(next, jb, found, 1, first, last, true, unwrap_next)?;
                }
            }

            Kind::Key(key) => {
                if jb.jtype() == JType::Object {
                    match jb.get_key(key) {
                        Some(v) => {
                            res = self.exec_next(next, v, found)?;
                        }
                        None => {
                            if !self.ignore_structural_errors {
                                if !self.throw_errors {
                                    return Ok(Jper::Error);
                                }
                                return Err(err(
                                    sqlstate::SQL_JSON_MEMBER_NOT_FOUND,
                                    format!("JSON object does not contain key \"{key}\""),
                                ));
                            }
                        }
                    }
                } else if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                } else if !self.ignore_structural_errors {
                    return_error!(
                        self,
                        err(
                            sqlstate::SQL_JSON_MEMBER_NOT_FOUND,
                            "jsonpath member accessor can only be applied to an object"
                        )
                    );
                }
            }

            Kind::Current => {
                let cur = self.current.clone();
                res = self.tail(F_EOUT, |s| s.exec_next(next, cur, found))?;
            }

            Kind::Root => {
                let jb = self.root.clone();
                let saved = self.set_base_object(&jb, 0);
                res = self.exec_next(next, jb, found)?;
                self.base = saved;
            }

            Kind::Filter(arg) => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                let st = self.exec_nested_bool_item(*arg, jb)?;
                if st != JpBool::True {
                    res = Jper::NotFound;
                } else {
                    let v = jb.clone();
                    res = self.tail(F_EOUT, |s| s.exec_next(next, v, found))?;
                }
            }

            Kind::Method(m) => {
                let m = *m;
                return self.exec_method(jsp, m, next, jb, found, unwrap);
            }

            Kind::Decimal(..) => {
                return self.exec_decimal_number(jsp, true, next, jb, found, unwrap);
            }

            Kind::DtMethod(..) => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                return self.tail(F_EOUT, |s| s.exec_datetime_method(jsp, jb, found));
            }

            Kind::Last => {
                if self.innermost_array_size < 0 {
                    return Err(err(
                        sqlstate::INTERNAL_ERROR,
                        "evaluating jsonpath LAST outside of array subscript",
                    ));
                }
                if next.is_none() && found.is_none() {
                    return Ok(Jper::Ok);
                }
                let last = i64::from(self.innermost_array_size) - 1;
                res = self.exec_next(next, num_val(JsonNumber::from_i64(last)), found)?;
            }
        }
        Ok(res)
    }

    /// The item methods of executeItemOptUnwrapTarget().
    fn exec_method(
        &mut self,
        jsp: NodeId,
        m: Method,
        next: Option<NodeId>,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
        unwrap: bool,
    ) -> Result<Jper> {
        let name = method_name(m);
        match m {
            Method::Type => {
                let v = Val::Str(Cow::Borrowed(jb.type_name()));
                self.tail(F_EOUT, |s| s.exec_next(next, v, found))
            }
            Method::Size => {
                let mut size = jb.array_size();
                if size < 0 {
                    if !self.lax {
                        if !self.ignore_structural_errors {
                            return_error!(
                                self,
                                err(
                                    sqlstate::SQL_JSON_ARRAY_NOT_FOUND,
                                    format!(
                                        "jsonpath item method .{name}() can only be applied to an array"
                                    )
                                )
                            );
                        }
                        return Ok(Jper::NotFound);
                    }
                    size = 1;
                }
                let v = num_val(JsonNumber::from_i64(i64::from(size)));
                self.tail(F_EOUT, |s| s.exec_next(next, v, found))
            }
            Method::Abs | Method::Floor | Method::Ceiling => self.tail(F_EOUT, |s| {
                s.exec_numeric_item_method(jsp, m, jb, found, unwrap)
            }),
            Method::Double => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                let v = match jb {
                    Val::Num(n) => {
                        let tmp = n.as_str();
                        match pgnumeric::float::parse_f64(tmp) {
                            Err(_) => return_error!(
                                self,
                                invalid_arg_error(tmp, name, "double precision")
                            ),
                            Ok(f) if !f.is_finite() => return_error!(self, nan_inf_error(name)),
                            Ok(_) => jb.clone(),
                        }
                    }
                    Val::Str(s) => match pgnumeric::float::parse_f64(s) {
                        Err(_) => {
                            return_error!(self, invalid_arg_error(s, name, "double precision"))
                        }
                        Ok(f) if !f.is_finite() => return_error!(self, nan_inf_error(name)),
                        Ok(f) => {
                            let n = Numeric::from_f64(f).map_err(exec_error)?;
                            num_val(from_numeric(n))
                        }
                    },
                    _ => return_error!(
                        self,
                        err(
                            sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                            format!(
                                "jsonpath item method .{name}() can only be applied to a string or numeric value"
                            )
                        )
                    ),
                };
                self.exec_next(next, v, found)
            }
            Method::KeyValue => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                self.tail(F_EOUT, |s| s.exec_keyvalue_method(jsp, jb, found))
            }
            Method::Date => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                self.tail(F_EOUT, |s| s.exec_datetime_method(jsp, jb, found))
            }
            Method::Bigint => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                let v = match jb {
                    Val::Num(n) => match n.as_numeric().to_i64() {
                        Ok(i) => i,
                        Err(_) => {
                            return_error!(self, invalid_arg_error(n.as_str(), name, "bigint"))
                        }
                    },
                    Val::Str(s) => match parse_int8(s) {
                        Some(i) => i,
                        None => return_error!(self, invalid_arg_error(s, name, "bigint")),
                    },
                    _ => return_error!(
                        self,
                        err(
                            sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                            format!(
                                "jsonpath item method .{name}() can only be applied to a string or numeric value"
                            )
                        )
                    ),
                };
                self.exec_next(next, num_val(JsonNumber::from_i64(v)), found)
            }
            Method::Boolean => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                let b = match jb {
                    Val::Bool(b) => *b,
                    Val::Num(n) => match crate::text::parse_int4(n.as_str()) {
                        Ok(i) => i != 0,
                        Err(_) => {
                            return_error!(self, invalid_arg_error(n.as_str(), name, "boolean"))
                        }
                    },
                    Val::Str(s) => match parse_bool(s) {
                        Some(b) => b,
                        None => return_error!(self, invalid_arg_error(s, name, "boolean")),
                    },
                    _ => return_error!(
                        self,
                        err(
                            sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                            format!(
                                "jsonpath item method .{name}() can only be applied to a boolean, string, or numeric value"
                            )
                        )
                    ),
                };
                self.exec_next(next, Val::Bool(b), found)
            }
            Method::Number => self.exec_decimal_number(jsp, false, next, jb, found, unwrap),
            Method::Integer => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                let v = match jb {
                    Val::Num(n) => match n.as_numeric().to_i32() {
                        Ok(i) => i,
                        Err(_) => {
                            return_error!(self, invalid_arg_error(n.as_str(), name, "integer"))
                        }
                    },
                    Val::Str(s) => match crate::text::parse_int4(s) {
                        Ok(i) => i,
                        Err(_) => return_error!(self, invalid_arg_error(s, name, "integer")),
                    },
                    _ => return_error!(
                        self,
                        err(
                            sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                            format!(
                                "jsonpath item method .{name}() can only be applied to a string or numeric value"
                            )
                        )
                    ),
                };
                self.exec_next(next, num_val(JsonNumber::from_i64(i64::from(v))), found)
            }
            Method::StringFunc => {
                if unwrap && jb.jtype() == JType::Array {
                    return self.tail(F_EOUT, |s| {
                        s.exec_unwrap_target_array(Some(jsp), jb, found, false)
                    });
                }
                let s: Cow<'a, str> = match jb {
                    Val::Str(s) => s.clone(),
                    Val::Num(n) => Cow::Owned(n.as_str().to_owned()),
                    Val::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
                    Val::DateTime(d) => Cow::Owned(d.to_json_string()?),
                    Val::Null | Val::Container(_) | Val::Kv(_) => return_error!(
                        self,
                        err(
                            sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                            format!(
                                "jsonpath item method .{name}() can only be applied to a boolean, string, numeric, or datetime value"
                            )
                        )
                    ),
                };
                self.exec_next(next, Val::Str(s), found)
            }
        }
    }

    /// The jpiDecimal / jpiNumber case of executeItemOptUnwrapTarget().
    fn exec_decimal_number(
        &mut self,
        jsp: NodeId,
        decimal: bool,
        next: Option<NodeId>,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
        unwrap: bool,
    ) -> Result<Jper> {
        if unwrap && jb.jtype() == JType::Array {
            return self.tail(F_EOUT, |s| {
                s.exec_unwrap_target_array(Some(jsp), jb, found, false)
            });
        }
        let name = if decimal { "decimal" } else { "number" };
        let (mut num, numstr): (Cow<'a, JsonNumber>, String) = match jb {
            Val::Num(n) => (n.clone(), n.as_str().to_owned()),
            Val::Str(s) => match Numeric::parse(s) {
                Err(_) => return_error!(self, invalid_arg_error(s, name, "numeric")),
                Ok(n) if !n.is_finite() => return_error!(self, nan_inf_error(name)),
                Ok(n) => (Cow::Owned(from_numeric(n)), s.to_string()),
            },
            _ => return_error!(
                self,
                err(
                    sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                    format!(
                        "jsonpath item method .{name}() can only be applied to a string or numeric value"
                    )
                )
            ),
        };
        // .decimal(precision [, scale]): convert to the typmod and apply it.
        if let Kind::Decimal(Some(p), s) = &self.node(jsp).kind {
            let precision = match self.literal_int4(*p) {
                Some(v) => v,
                None => return_error!(
                    self,
                    err(
                        sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                        format!(
                            "precision of jsonpath item method .{name}() is out of range for type integer"
                        )
                    )
                ),
            };
            let scale = match s {
                None => 0,
                Some(s) => match self.literal_int4(*s) {
                    Some(v) => v,
                    None => return_error!(
                        self,
                        err(
                            sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                            format!(
                                "scale of jsonpath item method .{name}() is out of range for type integer"
                            )
                        )
                    ),
                },
            };
            // make_numeric_typmod_safe()
            if !(1..=1000).contains(&precision) {
                if !self.throw_errors {
                    return Ok(Jper::Error);
                }
                return Err(err(
                    sqlstate::INVALID_PARAMETER_VALUE,
                    format!("NUMERIC precision {precision} must be between 1 and 1000"),
                ));
            }
            if !(-1000..=1000).contains(&scale) {
                if !self.throw_errors {
                    return Ok(Jper::Error);
                }
                return Err(err(
                    sqlstate::INVALID_PARAMETER_VALUE,
                    format!("NUMERIC scale {scale} must be between -1000 and 1000"),
                ));
            }
            match num.as_numeric().apply_typmod(precision, scale) {
                Ok(n) => num = Cow::Owned(from_numeric(n)),
                Err(_) => return_error!(self, invalid_arg_error(&numstr, name, "numeric")),
            }
        }
        self.exec_next(next, Val::Num(num), found)
    }

    /// numeric_int4_opt_error() on a numeric literal of the path.
    fn literal_int4(&self, id: NodeId) -> Option<i32> {
        match &self.node(id).kind {
            Kind::Numeric(n) => n.as_numeric().to_i32().ok(),
            _ => None,
        }
    }

    /// executeItemUnwrapTargetArray()
    fn exec_unwrap_target_array(
        &mut self,
        jsp: Option<NodeId>,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
        unwrap_elements: bool,
    ) -> Result<Jper> {
        self.enter(F_EUTA);
        let r = self.exec_any_item(jsp, jb, found, 1, 1, 1, false, unwrap_elements);
        self.leave(F_EUTA);
        r
    }

    /// executeNextItem(): execute the next item if there is one, otherwise
    /// add `v` to the result list.
    fn exec_next(
        &mut self,
        next: Option<NodeId>,
        v: Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        match next {
            Some(n) => {
                self.enter(F_ENI);
                let r = self.execute_item(n, &v, found);
                self.leave(F_ENI);
                r
            }
            None => {
                if let Some(f) = found {
                    f.push(v);
                }
                Ok(Jper::Ok)
            }
        }
    }

    /// executeItemOptUnwrapResult()
    fn exec_opt_unwrap_result(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        unwrap: bool,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        self.enter(F_EIOUR);
        let r = self.eiour_body(jsp, jb, unwrap, found);
        self.leave(F_EIOUR);
        r
    }

    fn eiour_body(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        unwrap: bool,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        if unwrap && self.lax {
            let mut seq = List::new();
            let res = self.execute_item(jsp, jb, Some(&mut seq))?;
            if res == Jper::Error {
                return Ok(res);
            }
            let found = found.expect("result list");
            for item in seq {
                if item.jtype() == JType::Array {
                    self.exec_unwrap_target_array(None, &item, Some(found), false)?;
                } else {
                    found.push(item);
                }
            }
            return Ok(Jper::Ok);
        }
        self.execute_item(jsp, jb, found)
    }

    /// executeItemOptUnwrapResultNoThrow()
    fn exec_opt_unwrap_result_no_throw(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        unwrap: bool,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        self.enter(F_NOTHROW);
        let throw_errors = self.throw_errors;
        self.throw_errors = false;
        let res = self.exec_opt_unwrap_result(jsp, jb, unwrap, found);
        self.throw_errors = throw_errors;
        self.leave(F_NOTHROW);
        res
    }

    /// executeBoolItem()
    fn exec_bool_item(&mut self, jsp: NodeId, jb: &Val<'a>, can_have_next: bool) -> Result<JpBool> {
        self.enter(F_EBI);
        self.check_stack()?;
        let r = self.ebi_body(jsp, jb, can_have_next);
        self.leave(F_EBI);
        r
    }

    fn ebi_body(&mut self, jsp: NodeId, jb: &Val<'a>, can_have_next: bool) -> Result<JpBool> {
        let node = self.node(jsp);
        if !can_have_next && node.next.is_some() {
            return Err(err(
                sqlstate::INTERNAL_ERROR,
                "boolean jsonpath item cannot have next item",
            ));
        }
        match &node.kind {
            Kind::Binary(BinOp::And, l, r) => {
                let res = self.exec_bool_item(*l, jb, false)?;
                if res == JpBool::False {
                    return Ok(JpBool::False);
                }
                // SQL/JSON says that we should check second arg in case of
                // jperError
                let res2 = self.exec_bool_item(*r, jb, false)?;
                Ok(if res2 == JpBool::True { res } else { res2 })
            }
            Kind::Binary(BinOp::Or, l, r) => {
                let res = self.exec_bool_item(*l, jb, false)?;
                if res == JpBool::True {
                    return Ok(JpBool::True);
                }
                let res2 = self.exec_bool_item(*r, jb, false)?;
                Ok(if res2 == JpBool::False { res } else { res2 })
            }
            Kind::Not(a) => {
                let res = self.exec_bool_item(*a, jb, false)?;
                Ok(match res {
                    JpBool::Unknown => JpBool::Unknown,
                    JpBool::True => JpBool::False,
                    JpBool::False => JpBool::True,
                })
            }
            Kind::IsUnknown(a) => {
                let res = self.exec_bool_item(*a, jb, false)?;
                Ok(if res == JpBool::Unknown {
                    JpBool::True
                } else {
                    JpBool::False
                })
            }
            Kind::Binary(
                op @ (BinOp::Equal
                | BinOp::NotEqual
                | BinOp::Less
                | BinOp::Greater
                | BinOp::LessOrEqual
                | BinOp::GreaterOrEqual),
                l,
                r,
            ) => self.exec_predicate(*l, Some(*r), jb, true, Pred::Comparison(*op), jsp),
            Kind::Binary(BinOp::StartsWith, l, r) => {
                self.exec_predicate(*l, Some(*r), jb, false, Pred::StartsWith, jsp)
            }
            Kind::LikeRegex { expr, .. } => {
                self.exec_predicate(*expr, None, jb, false, Pred::LikeRegex, jsp)
            }
            Kind::Exists(a) => {
                if !self.lax {
                    // In strict mode we must get a complete list of values to
                    // check that there are no errors at all.
                    let mut vals = List::new();
                    let res =
                        self.exec_opt_unwrap_result_no_throw(*a, jb, false, Some(&mut vals))?;
                    if res == Jper::Error {
                        return Ok(JpBool::Unknown);
                    }
                    Ok(if vals.is_empty() {
                        JpBool::False
                    } else {
                        JpBool::True
                    })
                } else {
                    let res = self.exec_opt_unwrap_result_no_throw(*a, jb, false, None)?;
                    Ok(match res {
                        Jper::Error => JpBool::Unknown,
                        Jper::Ok => JpBool::True,
                        Jper::NotFound => JpBool::False,
                    })
                }
            }
            _ => Err(err(
                sqlstate::INTERNAL_ERROR,
                "invalid boolean jsonpath item type",
            )),
        }
    }

    /// executeNestedBoolItem(): evaluates a filter predicate with `@` set to
    /// the current item.
    fn exec_nested_bool_item(&mut self, jsp: NodeId, jb: &Val<'a>) -> Result<JpBool> {
        self.enter(F_ENBI);
        let prev = std::mem::replace(&mut self.current, jb.clone());
        let res = self.exec_bool_item(jsp, jb, false);
        self.current = prev;
        self.leave(F_ENBI);
        res
    }

    /// executeAnyItem(): .** (jpiAny), .* (jpiAnyKey) and [*] (jpiAnyArray).
    #[allow(clippy::too_many_arguments)]
    fn exec_any_item(
        &mut self,
        jsp: Option<NodeId>,
        jbc: &Val<'a>,
        mut found: Option<&mut List<'a>>,
        level: u32,
        first: u32,
        last: u32,
        ignore_structural_errors: bool,
        unwrap_next: bool,
    ) -> Result<Jper> {
        self.enter(F_EAI);
        self.check_stack()?;
        let mut res = Jper::NotFound;
        if level > last {
            self.leave(F_EAI);
            return Ok(res);
        }
        for v in jbc.children() {
            let is_container = matches!(v, Val::Container(_) | Val::Kv(_));
            if level >= first || (first == u32::MAX && last == u32::MAX && !is_container) {
                // check expression
                if let Some(jsp) = jsp {
                    if ignore_structural_errors {
                        let saved = self.ignore_structural_errors;
                        self.ignore_structural_errors = true;
                        res = self.exec_opt_unwrap_target(
                            jsp,
                            &v,
                            found.as_deref_mut(),
                            unwrap_next,
                        )?;
                        self.ignore_structural_errors = saved;
                    } else {
                        res = self.exec_opt_unwrap_target(
                            jsp,
                            &v,
                            found.as_deref_mut(),
                            unwrap_next,
                        )?;
                    }
                    if res == Jper::Error || (res == Jper::Ok && found.is_none()) {
                        break;
                    }
                } else if let Some(f) = found.as_deref_mut() {
                    f.push(v.clone());
                } else {
                    self.leave(F_EAI);
                    return Ok(Jper::Ok);
                }
            }
            if level < last && is_container {
                res = self.exec_any_item(
                    jsp,
                    &v,
                    found.as_deref_mut(),
                    level + 1,
                    first,
                    last,
                    ignore_structural_errors,
                    unwrap_next,
                )?;
                if res == Jper::Error || (res == Jper::Ok && found.is_none()) {
                    break;
                }
            }
        }
        self.leave(F_EAI);
        Ok(res)
    }

    /// executePredicate(): existence semantics over the operand sequences.
    fn exec_predicate(
        &mut self,
        larg: NodeId,
        rarg: Option<NodeId>,
        jb: &Val<'a>,
        unwrap_right_arg: bool,
        pred: Pred,
        jsp: NodeId,
    ) -> Result<JpBool> {
        self.enter(F_EP);
        let r = self.ep_body(larg, rarg, jb, unwrap_right_arg, pred, jsp);
        self.leave(F_EP);
        r
    }

    fn ep_body(
        &mut self,
        larg: NodeId,
        rarg: Option<NodeId>,
        jb: &Val<'a>,
        unwrap_right_arg: bool,
        pred: Pred,
        jsp: NodeId,
    ) -> Result<JpBool> {
        let mut lseq = List::new();
        let mut rseq = List::new();
        // Left argument is always auto-unwrapped.
        let res = self.exec_opt_unwrap_result_no_throw(larg, jb, true, Some(&mut lseq))?;
        if res == Jper::Error {
            return Ok(JpBool::Unknown);
        }
        if let Some(rarg) = rarg {
            // Right argument is conditionally auto-unwrapped.
            let res =
                self.exec_opt_unwrap_result_no_throw(rarg, jb, unwrap_right_arg, Some(&mut rseq))?;
            if res == Jper::Error {
                return Ok(JpBool::Unknown);
            }
        }
        let mut error = false;
        let mut found = false;
        for lval in &lseq {
            let mut ri = 0;
            let mut first = true;
            // Loop over right arg sequence or do single pass otherwise
            while if rarg.is_some() {
                ri < rseq.len()
            } else {
                first
            } {
                let rval = if rarg.is_some() {
                    Some(&rseq[ri])
                } else {
                    None
                };
                let res = match pred {
                    Pred::Comparison(op) => self.compare_items(op, lval, rval.unwrap())?,
                    Pred::StartsWith => execute_starts_with(lval, rval.unwrap()),
                    Pred::LikeRegex => self.execute_like_regex(jsp, lval)?,
                };
                if res == JpBool::Unknown {
                    if !self.lax {
                        return Ok(JpBool::Unknown);
                    }
                    error = true;
                } else if res == JpBool::True {
                    if self.lax {
                        return Ok(JpBool::True);
                    }
                    found = true;
                }
                first = false;
                ri += 1;
            }
        }
        if found {
            // possible only in strict mode
            return Ok(JpBool::True);
        }
        if error {
            // possible only in lax mode
            return Ok(JpBool::Unknown);
        }
        Ok(JpBool::False)
    }

    /// executeBinaryArithmExpr()
    fn exec_binary_arithm(
        &mut self,
        jsp: NodeId,
        op: BinOp,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        self.enter(F_EBAE);
        let r = self.ebae_body(jsp, op, jb, found);
        self.leave(F_EBAE);
        r
    }

    fn ebae_body(
        &mut self,
        jsp: NodeId,
        op: BinOp,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        let node = self.node(jsp);
        let (l, r) = match node.kind {
            Kind::Binary(_, l, r) => (l, r),
            _ => unreachable!(),
        };
        let mut lseq = List::new();
        let mut rseq = List::new();
        // XXX: By standard only operands of multiplicative expressions are
        // unwrapped. We extend it to other binary arithmetic expressions too.
        let jper = self.exec_opt_unwrap_result(l, jb, true, Some(&mut lseq))?;
        if jper == Jper::Error {
            return Ok(jper);
        }
        let jper = self.exec_opt_unwrap_result(r, jb, true, Some(&mut rseq))?;
        if jper == Jper::Error {
            return Ok(jper);
        }
        let name = binop_name(op);
        let lval = match (lseq.len(), lseq.first()) {
            (1, Some(Val::Num(n))) => n,
            _ => return_error!(
                self,
                err(
                    sqlstate::SINGLETON_SQL_JSON_ITEM_REQUIRED,
                    format!(
                        "left operand of jsonpath operator {name} is not a single numeric value"
                    )
                )
            ),
        };
        let rval = match (rseq.len(), rseq.first()) {
            (1, Some(Val::Num(n))) => n,
            _ => return_error!(
                self,
                err(
                    sqlstate::SINGLETON_SQL_JSON_ITEM_REQUIRED,
                    format!(
                        "right operand of jsonpath operator {name} is not a single numeric value"
                    )
                )
            ),
        };
        let (a, b) = (lval.as_numeric(), rval.as_numeric());
        let r = match op {
            BinOp::Add => a.add(&b),
            BinOp::Sub => a.sub(&b),
            BinOp::Mul => a.mul(&b),
            BinOp::Div => a.div(&b),
            BinOp::Mod => a.rem(&b),
            _ => unreachable!(),
        };
        let res = match r {
            Ok(n) => n,
            Err(e) => {
                if self.throw_errors {
                    return Err(exec_error(e));
                }
                return Ok(Jper::Error);
            }
        };
        let next = node.next;
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok);
        }
        self.exec_next(next, num_val(from_numeric(res)), found)
    }

    /// executeUnaryArithmExpr()
    fn exec_unary_arithm(
        &mut self,
        jsp: NodeId,
        minus: bool,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        self.enter(F_EUAE);
        let r = self.euae_body(jsp, minus, jb, found);
        self.leave(F_EUAE);
        r
    }

    fn euae_body(
        &mut self,
        jsp: NodeId,
        minus: bool,
        jb: &Val<'a>,
        mut found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        let node = self.node(jsp);
        let arg = match node.kind {
            Kind::Plus(a) | Kind::Minus(a) => a,
            _ => unreachable!(),
        };
        let mut seq = List::new();
        let jper = self.exec_opt_unwrap_result(arg, jb, true, Some(&mut seq))?;
        if jper == Jper::Error {
            return Ok(jper);
        }
        let mut jper = Jper::NotFound;
        let next = node.next;
        for val in seq {
            let val = match val {
                Val::Num(n) => {
                    if found.is_none() && next.is_none() {
                        return Ok(Jper::Ok);
                    }
                    if minus {
                        num_val(n.negate())
                    } else {
                        Val::Num(n)
                    }
                }
                _ => {
                    if found.is_none() && next.is_none() {
                        continue; // skip non-numerics processing
                    }
                    return_error!(
                        self,
                        err(
                            sqlstate::SQL_JSON_NUMBER_NOT_FOUND,
                            format!(
                                "operand of unary jsonpath operator {} is not a numeric value",
                                if minus { "-" } else { "+" }
                            )
                        )
                    );
                }
            };
            let jper2 = self.exec_next(next, val, found.as_deref_mut())?;
            if jper2 == Jper::Error {
                return Ok(jper2);
            }
            if jper2 == Jper::Ok {
                if found.is_none() {
                    return Ok(Jper::Ok);
                }
                jper = Jper::Ok;
            }
        }
        Ok(jper)
    }

    /// LIKE_REGEX predicate callback.
    fn execute_like_regex(&self, jsp: NodeId, s: &Val<'a>) -> Result<JpBool> {
        let Some(s) = s.str() else {
            return Ok(JpBool::Unknown);
        };
        let Kind::LikeRegex { regex, .. } = &self.node(jsp).kind else {
            unreachable!()
        };
        Ok(if regex.try_is_match(s)? {
            JpBool::True
        } else {
            JpBool::False
        })
    }

    /// executeNumericItemMethod(): .abs(), .floor(), .ceiling()
    fn exec_numeric_item_method(
        &mut self,
        jsp: NodeId,
        m: Method,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
        unwrap: bool,
    ) -> Result<Jper> {
        self.enter(F_ENIM);
        let r = self.enim_body(jsp, m, jb, found, unwrap);
        self.leave(F_ENIM);
        r
    }

    fn enim_body(
        &mut self,
        jsp: NodeId,
        m: Method,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
        unwrap: bool,
    ) -> Result<Jper> {
        if unwrap && jb.jtype() == JType::Array {
            return self.tail(F_ENIM, |s| {
                s.exec_unwrap_target_array(Some(jsp), jb, found, false)
            });
        }
        let Some(n) = jb.num() else {
            return_error!(
                self,
                err(
                    sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
                    format!(
                        "jsonpath item method .{}() can only be applied to a numeric value",
                        method_name(m)
                    )
                )
            );
        };
        let v = n.as_numeric();
        let datum = match m {
            Method::Abs => v.abs(),
            Method::Floor => v.try_floor().map_err(exec_error)?,
            _ => v.try_ceil().map_err(exec_error)?,
        };
        let next = self.node(jsp).next;
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok);
        }
        self.exec_next(next, num_val(from_numeric(datum)), found)
    }

    /// executeDateTimeMethod()
    fn exec_datetime_method(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        self.enter(F_EDTM);
        let r = self.edtm_body(jsp, jb, found);
        self.leave(F_EDTM);
        r
    }

    fn edtm_body(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        let node = self.node(jsp);
        let (method, name, arg) = match &node.kind {
            Kind::DtMethod(m, arg) => (datetime::Target::from_dt(*m), dt_method_name(*m), *arg),
            Kind::Method(Method::Date) => (datetime::Target::Date, "date", None),
            _ => unreachable!(),
        };
        let Some(s) = jb.str() else {
            return_error!(
                self,
                err(
                    sqlstate::INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION,
                    format!("jsonpath item method .{name}() can only be applied to a string")
                )
            );
        };
        let arg = arg.map(|a| &self.node(a).kind);
        let value = match datetime::execute(s, method, name, arg, self.use_tz, self.tz)? {
            Ok(v) => v,
            Err(e) => return_error!(self, e),
        };
        let next = node.next;
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok);
        }
        self.exec_next(next, Val::DateTime(value), found)
    }

    /// executeKeyValueMethod(): `{"key": key, "value": value, "id": id}`
    /// for each pair of an object.
    fn exec_keyvalue_method(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        self.enter(F_EKVM);
        let r = self.ekvm_body(jsp, jb, found);
        self.leave(F_EKVM);
        r
    }

    fn ekvm_body(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
        mut found: Option<&mut List<'a>>,
    ) -> Result<Jper> {
        if jb.jtype() != JType::Object {
            return_error!(
                self,
                err(
                    sqlstate::SQL_JSON_OBJECT_NOT_FOUND,
                    "jsonpath item method .keyvalue() can only be applied to an object"
                )
            );
        }
        if jb.object_len() == 0 {
            return Ok(Jper::NotFound); // no key-value pairs
        }
        let next = self.node(jsp).next;
        // construct object id from its base object and offset inside that
        let id = self
            .container_offset(jb)
            .wrapping_add(i64::from(self.base.id).wrapping_mul(10_000_000_000));
        let mut res = Jper::NotFound;
        for (key, value) in jb.pairs() {
            res = Jper::Ok;
            if next.is_none() && found.is_none() {
                break;
            }
            let obj = Rc::new(KvObj { key, value, id });
            let gen_id = self.last_generated_object_id;
            self.last_generated_object_id = gen_id.wrapping_add(1);
            let saved = std::mem::replace(
                &mut self.base,
                Base {
                    root: BaseRoot::Kv(obj.clone()),
                    id: gen_id,
                },
            );
            res = self.exec_next(next, Val::Kv(obj), found.as_deref_mut())?;
            self.base = saved;
            if res == Jper::Error {
                return Ok(res);
            }
            if res == Jper::Ok && found.is_none() {
                break;
            }
        }
        Ok(res)
    }

    /// Byte offset of a container within the binary jsonb of the current
    /// base object, as `(char *) jbc - (char *) cxt->baseObject.jbc`.
    fn container_offset(&mut self, jb: &Val<'a>) -> i64 {
        match (&self.base.root, jb) {
            (BaseRoot::Json(root), Val::Container(c)) => self.offsets.offset(root, c).unwrap_or(0),
            (BaseRoot::Kv(kv), Val::Kv(o)) if Rc::ptr_eq(kv, o) => 0,
            (BaseRoot::Kv(kv), Val::Container(c)) => self
                .offsets
                .kv_offset(&kv.key, &kv.value, kv.id, c)
                .unwrap_or(0),
            _ => 0,
        }
    }

    /// appendBoolResult()
    fn append_bool_result(
        &mut self,
        jsp: NodeId,
        found: Option<&mut List<'a>>,
        res: JpBool,
    ) -> Result<Jper> {
        let next = self.node(jsp).next;
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok); // found singleton boolean value
        }
        let v = match res {
            JpBool::Unknown => Val::Null,
            JpBool::True => Val::Bool(true),
            JpBool::False => Val::Bool(false),
        };
        self.exec_next(next, v, found)
    }

    /// getJsonPathItem(): a literal or a variable.
    fn get_jsonpath_item(&mut self, jsp: NodeId) -> Result<Val<'a>> {
        Ok(match &self.node(jsp).kind {
            Kind::Null => Val::Null,
            Kind::Bool(b) => Val::Bool(*b),
            Kind::Numeric(n) => Val::Num(Cow::Borrowed(n)),
            Kind::String(s) => Val::Str(Cow::Borrowed(s.as_str())),
            Kind::Variable(name) => {
                let v = self
                    .vars
                    .and_then(|vars| vars.get_key(name).map(|v| (vars, v)));
                match v {
                    None => {
                        return Err(err(
                            sqlstate::UNDEFINED_OBJECT,
                            format!("could not find jsonpath variable \"{name}\""),
                        ));
                    }
                    Some((vars, v)) => {
                        self.base = Base {
                            root: BaseRoot::Json(vars),
                            id: 1,
                        };
                        Val::from_jsonb(v)
                    }
                }
            }
            _ => unreachable!("unexpected jsonpath item type"),
        })
    }

    /// setBaseObject(): returns the previous base object.
    fn set_base_object(&mut self, jbv: &Val<'a>, id: i32) -> Base<'a> {
        let root = match jbv {
            Val::Container(c) => BaseRoot::Json(c),
            Val::Kv(kv) => BaseRoot::Kv(kv.clone()),
            _ => BaseRoot::None,
        };
        std::mem::replace(&mut self.base, Base { root, id })
    }

    /// getArrayIndex(): evaluates a subscript and truncates it to int4.
    /// Ok(Err(jper)) when the result is a (suppressed) error.
    fn get_array_index(
        &mut self,
        jsp: NodeId,
        jb: &Val<'a>,
    ) -> Result<std::result::Result<i32, Jper>> {
        self.enter(F_GAI);
        let r = self.gai_body(jsp, jb);
        self.leave(F_GAI);
        r
    }

    fn gai_body(&mut self, jsp: NodeId, jb: &Val<'a>) -> Result<std::result::Result<i32, Jper>> {
        let mut found = List::new();
        let res = self.execute_item(jsp, jb, Some(&mut found))?;
        if res == Jper::Error {
            return Ok(Err(res));
        }
        let n = match (found.len(), found.first()) {
            (1, Some(Val::Num(n))) => n,
            _ => {
                if self.throw_errors {
                    return Err(err(
                        sqlstate::INVALID_SQL_JSON_SUBSCRIPT,
                        "jsonpath array subscript is not a single numeric value",
                    ));
                }
                return Ok(Err(Jper::Error));
            }
        };
        let t = n.as_numeric().trunc(0).map_err(exec_error)?;
        match t.to_i32() {
            Ok(i) => Ok(Ok(i)),
            Err(_) => {
                if self.throw_errors {
                    return Err(err(
                        sqlstate::INVALID_SQL_JSON_SUBSCRIPT,
                        "jsonpath array subscript is out of integer range",
                    ));
                }
                Ok(Err(Jper::Error))
            }
        }
    }

    /// compareItems()
    fn compare_items(&self, op: BinOp, jb1: &Val<'a>, jb2: &Val<'a>) -> Result<JpBool> {
        if jb1.raw_type() != jb2.raw_type() {
            if matches!(jb1, Val::Null) || matches!(jb2, Val::Null) {
                // Equality and order comparison of nulls to non-nulls
                // returns always false, but inequality comparison returns
                // true.
                return Ok(if op == BinOp::NotEqual {
                    JpBool::True
                } else {
                    JpBool::False
                });
            }
            // Non-null items of different types are not comparable.
            return Ok(JpBool::Unknown);
        }
        let cmp = match (jb1, jb2) {
            (Val::Null, Val::Null) => 0,
            (Val::Bool(a), Val::Bool(b)) => (*a as i32) - (*b as i32),
            (Val::Num(a), Val::Num(b)) => a.as_ref().cmp(b.as_ref()) as i32,
            (Val::Str(a), Val::Str(b)) => {
                if op == BinOp::Equal {
                    return Ok(if a == b { JpBool::True } else { JpBool::False });
                }
                a.as_bytes().cmp(b.as_bytes()) as i32
            }
            (Val::DateTime(a), Val::DateTime(b)) => {
                match datetime::compare(a, b, self.use_tz, self.tz)? {
                    Some(c) => c,
                    None => return Ok(JpBool::Unknown),
                }
            }
            // non-scalars are not comparable
            _ => return Ok(JpBool::Unknown),
        };
        let res = match op {
            BinOp::Equal => cmp == 0,
            BinOp::NotEqual => cmp != 0,
            BinOp::Less => cmp < 0,
            BinOp::Greater => cmp > 0,
            BinOp::LessOrEqual => cmp <= 0,
            BinOp::GreaterOrEqual => cmp >= 0,
            _ => unreachable!(),
        };
        Ok(if res { JpBool::True } else { JpBool::False })
    }
}

/// STARTS_WITH predicate callback.
fn execute_starts_with(whole: &Val<'_>, initial: &Val<'_>) -> JpBool {
    let (Some(whole), Some(initial)) = (whole.str(), initial.str()) else {
        return JpBool::Unknown;
    };
    if whole.as_bytes().starts_with(initial.as_bytes()) {
        JpBool::True
    } else {
        JpBool::False
    }
}

fn invalid_arg_error(arg: &str, method: &str, typname: &str) -> Error {
    err(
        sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
        format!(
            "argument \"{arg}\" of jsonpath item method .{method}() is invalid for type {typname}"
        ),
    )
}

fn nan_inf_error(method: &str) -> Error {
    err(
        sqlstate::NON_NUMERIC_SQL_JSON_ITEM,
        format!("NaN or Infinity is not allowed for jsonpath item method .{method}()"),
    )
}

/// parse_bool(): true/false/yes/no/on/off/1/0 and unique prefixes,
/// case-insensitive, no surrounding whitespace.
fn parse_bool(s: &str) -> Option<bool> {
    let b = s.as_bytes();
    let len = b.len();
    // pg_strncasecmp(value, word, len) == 0
    let prefix_of = |word: &str, n: usize| -> bool {
        let w = word.as_bytes();
        for i in 0..n {
            let c = b.get(i).copied().unwrap_or(0).to_ascii_lowercase();
            let d = w.get(i).copied().unwrap_or(0);
            if c != d {
                return false;
            }
            if c == 0 {
                return true;
            }
        }
        true
    };
    match b.first().map(|c| c.to_ascii_lowercase()) {
        Some(b't') if prefix_of("true", len) => Some(true),
        Some(b'f') if prefix_of("false", len) => Some(false),
        Some(b'y') if prefix_of("yes", len) => Some(true),
        Some(b'n') if prefix_of("no", len) => Some(false),
        Some(b'o') => {
            if prefix_of("on", len.max(2)) {
                Some(true)
            } else if prefix_of("off", len.max(2)) {
                Some(false)
            } else {
                None
            }
        }
        Some(b'1') if len == 1 => Some(true),
        Some(b'0') if len == 1 => Some(false),
        _ => None,
    }
}

/// int8in() / pg_strtoint64_safe(): None on any error.
fn parse_int8(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let at = |i: usize| -> u8 { b.get(i).copied().unwrap_or(0) };
    let is_space = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c);
    let mut p = 0;
    while is_space(at(p)) {
        p += 1;
    }
    let mut neg = false;
    if at(p) == b'-' {
        neg = true;
        p += 1;
    } else if at(p) == b'+' {
        p += 1;
    }
    let base: u64 = if at(p) == b'0' {
        match at(p + 1) {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 10,
        }
    } else {
        10
    };
    if base != 10 {
        p += 2;
    }
    let digit = |c: u8| -> Option<u64> {
        let d = (c as char).to_digit(16)? as u64;
        if d < base { Some(d) } else { None }
    };
    let firstdigit = p;
    let mut tmp: u64 = 0;
    let limit: u64 = 1u64 << 63; // -(INT64_MIN)
    loop {
        let c = at(p);
        if let Some(d) = digit(c) {
            tmp = tmp.checked_mul(base)?.checked_add(d)?;
            if tmp > limit {
                return None;
            }
            p += 1;
        } else if c == b'_' {
            // underscore must be preceded and followed by a digit
            if p == firstdigit {
                return None;
            }
            p += 1;
            digit(at(p))?;
        } else {
            break;
        }
    }
    if p == firstdigit {
        return None;
    }
    while is_space(at(p)) {
        p += 1;
    }
    if p != b.len() {
        return None;
    }
    if neg {
        Some((tmp as i128).wrapping_neg() as i64)
    } else if tmp > i64::MAX as u64 {
        None
    } else {
        Some(tmp as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bools() {
        for (s, r) in [
            ("t", Some(true)),
            ("TRUE", Some(true)),
            ("tru", Some(true)),
            ("truex", None),
            ("f", Some(false)),
            ("o", None),
            ("on", Some(true)),
            ("of", Some(false)),
            ("off", Some(false)),
            ("offf", None),
            ("1", Some(true)),
            ("0", Some(false)),
            ("01", None),
            ("", None),
            (" true", None),
            ("y", Some(true)),
            ("no", Some(false)),
        ] {
            assert_eq!(parse_bool(s), r, "{s}");
        }
    }

    #[test]
    fn int8() {
        assert_eq!(parse_int8(" 12 "), Some(12));
        assert_eq!(parse_int8("-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_int8("9223372036854775808"), None);
        assert_eq!(parse_int8("0x7FFF_FFFF_FFFF_FFFF"), Some(i64::MAX));
        assert_eq!(parse_int8("1_000"), Some(1000));
        assert_eq!(parse_int8("_1"), None);
        assert_eq!(parse_int8("1_"), None);
        assert_eq!(parse_int8("1.5"), None);
        assert_eq!(parse_int8(""), None);
        assert_eq!(parse_int8("0x"), None);
    }
}
