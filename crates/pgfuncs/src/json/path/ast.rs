//! jsonpath parse tree, the checks of flattenJsonPathParseItem() and the
//! text output of printJsonPathItem(): ports of
//! src/backend/utils/adt/jsonpath.c of PostgreSQL 17 (Copyright (c)
//! 2019-2024, PostgreSQL Global Development Group; PostgreSQL License).
//!
//! Nodes live in an arena (`Vec<Node>`) and refer to each other by index,
//! like the items of PostgreSQL's binary jsonpath format: a path item has an
//! optional `next` item and possibly argument items. All traversals here are
//! iterative, so arbitrarily long or deeply nested paths are safe.

use crate::json::JsonNumber;
use crate::json::output::escape_json;
use crate::pattern::Regex;
use crate::{Error, Result, sqlstate};

pub(crate) type NodeId = u32;

/// Binary operators and predicates (jpiAnd .. jpiMod, jpiStartsWith).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BinOp {
    And,
    Or,
    Equal,
    NotEqual,
    Less,
    Greater,
    LessOrEqual,
    GreaterOrEqual,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    StartsWith,
}

/// Item methods without arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    Type,
    Size,
    Abs,
    Floor,
    Ceiling,
    Double,
    KeyValue,
    Bigint,
    Boolean,
    Date,
    Integer,
    Number,
    StringFunc,
}

/// Datetime item methods with an optional argument (template for
/// `.datetime()`, time precision for the others).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DtMethod {
    Datetime,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
}

/// XQuery regex flags of LIKE_REGEX.
pub(crate) const JSP_REGEX_ICASE: u32 = 0x01;
pub(crate) const JSP_REGEX_DOTALL: u32 = 0x02;
pub(crate) const JSP_REGEX_MLINE: u32 = 0x04;
pub(crate) const JSP_REGEX_WSPACE: u32 = 0x08;
pub(crate) const JSP_REGEX_QUOTE: u32 = 0x10;

#[derive(Clone, Debug)]
pub(crate) enum Kind {
    Null,
    String(String),
    Numeric(JsonNumber),
    Bool(bool),
    Binary(BinOp, NodeId, NodeId),
    Not(NodeId),
    IsUnknown(NodeId),
    Plus(NodeId),
    Minus(NodeId),
    AnyArray,
    AnyKey,
    /// subscripts: `from` or `from to to`
    IndexArray(Vec<(NodeId, Option<NodeId>)>),
    /// `.**{first to last}`; u32::MAX stands for `last`
    Any {
        first: u32,
        last: u32,
    },
    Key(String),
    Current,
    Root,
    Variable(String),
    Filter(NodeId),
    Exists(NodeId),
    Method(Method),
    DtMethod(DtMethod, Option<NodeId>),
    /// `.decimal([precision [, scale]])`
    Decimal(Option<NodeId>, Option<NodeId>),
    Last,
    LikeRegex {
        expr: NodeId,
        pattern: String,
        flags: u32,
        regex: Regex,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub kind: Kind,
    pub next: Option<NodeId>,
}

/// jspOperationName()
pub(crate) fn binop_name(op: BinOp) -> &'static str {
    match op {
        BinOp::And => "&&",
        BinOp::Or => "||",
        BinOp::Equal => "==",
        BinOp::NotEqual => "!=",
        BinOp::Less => "<",
        BinOp::Greater => ">",
        BinOp::LessOrEqual => "<=",
        BinOp::GreaterOrEqual => ">=",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::StartsWith => "starts with",
    }
}

pub(crate) fn method_name(m: Method) -> &'static str {
    match m {
        Method::Type => "type",
        Method::Size => "size",
        Method::Abs => "abs",
        Method::Floor => "floor",
        Method::Ceiling => "ceiling",
        Method::Double => "double",
        Method::KeyValue => "keyvalue",
        Method::Bigint => "bigint",
        Method::Boolean => "boolean",
        Method::Date => "date",
        Method::Integer => "integer",
        Method::Number => "number",
        Method::StringFunc => "string",
    }
}

pub(crate) fn dt_method_name(m: DtMethod) -> &'static str {
    match m {
        DtMethod::Datetime => "datetime",
        DtMethod::Time => "time",
        DtMethod::TimeTz => "time_tz",
        DtMethod::Timestamp => "timestamp",
        DtMethod::TimestampTz => "timestamp_tz",
    }
}

/// operationPriority()
fn priority(k: &Kind) -> u8 {
    match k {
        Kind::Binary(op, ..) => match op {
            BinOp::Or => 0,
            BinOp::And => 1,
            BinOp::Equal
            | BinOp::NotEqual
            | BinOp::Less
            | BinOp::Greater
            | BinOp::LessOrEqual
            | BinOp::GreaterOrEqual
            | BinOp::StartsWith => 2,
            BinOp::Add | BinOp::Sub => 3,
            BinOp::Mul | BinOp::Div | BinOp::Mod => 4,
        },
        Kind::Plus(_) | Kind::Minus(_) => 5,
        _ => 6,
    }
}

/// Recursion depth (in PostgreSQL's flattenJsonPathParseItem() and
/// printJsonPathItem(), which recurse into arguments and into the next item)
/// at which `check_stack_depth()` fails with PostgreSQL 17's default 2MB
/// `max_stack_depth` (measured on x86-64 in a top-level query; the exact
/// cut-off varies by a few levels with the caller's stack usage): parsing
/// accepts depth 14544, output accepts depth 11897.
pub(crate) const FLATTEN_MAX_DEPTH: u32 = 14544;
pub(crate) const PRINT_MAX_DEPTH: u32 = 11897;

pub(crate) fn stack_depth_error() -> Error {
    Error::new(
        sqlstate::STATEMENT_TOO_COMPLEX,
        "stack depth limit exceeded",
    )
}

/// The checks of flattenJsonPathParseItem(), in its traversal order (item,
/// its arguments, then its next item): `@` outside filters, `last` outside
/// array subscripts, and the recursion depth limit. Returns the maximum
/// recursion depth.
pub(crate) fn check_tree(nodes: &[Node], root: NodeId) -> Result<u32> {
    // (node, depth, nesting level, inside array subscript)
    let mut stack: Vec<(NodeId, u32, u32, bool)> = vec![(root, 1, 0, false)];
    let mut max_depth = 0;
    while let Some((id, depth, nesting, in_sub)) = stack.pop() {
        if depth > FLATTEN_MAX_DEPTH {
            return Err(stack_depth_error());
        }
        max_depth = max_depth.max(depth);
        let node = &nodes[id as usize];
        // children are visited in reverse push order
        if let Some(next) = node.next {
            stack.push((next, depth + 1, nesting, in_sub));
        }
        let d = depth + 1;
        match &node.kind {
            Kind::Binary(_, l, r) => {
                stack.push((*r, d, nesting, in_sub));
                stack.push((*l, d, nesting, in_sub));
            }
            Kind::Decimal(l, r) => {
                if let Some(r) = r {
                    stack.push((*r, d, nesting, in_sub));
                }
                if let Some(l) = l {
                    stack.push((*l, d, nesting, in_sub));
                }
            }
            Kind::LikeRegex { expr, .. } => stack.push((*expr, d, nesting, in_sub)),
            Kind::Filter(a) => stack.push((*a, d, nesting + 1, in_sub)),
            Kind::Not(a)
            | Kind::IsUnknown(a)
            | Kind::Plus(a)
            | Kind::Minus(a)
            | Kind::Exists(a) => stack.push((*a, d, nesting, in_sub)),
            Kind::DtMethod(_, Some(a)) => stack.push((*a, d, nesting, in_sub)),
            Kind::Current => {
                if nesting == 0 {
                    return Err(Error::new(
                        sqlstate::SYNTAX_ERROR,
                        "@ is not allowed in root expressions",
                    ));
                }
            }
            Kind::Last => {
                if !in_sub {
                    return Err(Error::new(
                        sqlstate::SYNTAX_ERROR,
                        "LAST is allowed only in array subscripts",
                    ));
                }
            }
            Kind::IndexArray(subs) => {
                for (from, to) in subs.iter().rev() {
                    if let Some(to) = to {
                        stack.push((*to, d, nesting, true));
                    }
                    stack.push((*from, d, nesting, true));
                }
            }
            _ => {}
        }
    }
    Ok(max_depth)
}

/// Pending output of the iterative printJsonPathItem().
enum Task<'a> {
    /// print item `id`
    Item {
        id: NodeId,
        in_key: bool,
        brackets: bool,
    },
    Str(&'a str),
    Owned(String),
    Char(char),
}

/// printJsonPathItem() for the whole path (without the `strict ` prefix).
pub(crate) fn print(nodes: &[Node], root: NodeId, out: &mut String) {
    let mut stack = vec![Task::Item {
        id: root,
        in_key: false,
        brackets: true,
    }];
    while let Some(task) = stack.pop() {
        let (id, in_key, brackets) = match task {
            Task::Str(s) => {
                out.push_str(s);
                continue;
            }
            Task::Owned(s) => {
                out.push_str(&s);
                continue;
            }
            Task::Char(c) => {
                out.push(c);
                continue;
            }
            Task::Item {
                id,
                in_key,
                brackets,
            } => (id, in_key, brackets),
        };
        let node = &nodes[id as usize];
        // Tasks are pushed in reverse order of output: first the next item.
        if let Some(next) = node.next {
            stack.push(Task::Item {
                id: next,
                in_key: true,
                brackets: true,
            });
        }
        let item = |id: NodeId, brackets: bool| Task::Item {
            id,
            in_key: false,
            brackets,
        };
        match &node.kind {
            Kind::Null => out.push_str("null"),
            Kind::String(s) => {
                let _ = escape_json(out, s);
            }
            Kind::Numeric(n) => {
                if node.next.is_some() {
                    out.push('(');
                    out.push_str(n.as_str());
                    out.push(')');
                } else {
                    out.push_str(n.as_str());
                }
            }
            Kind::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Kind::Binary(op, l, r) => {
                let p = priority(&node.kind);
                if brackets {
                    out.push('(');
                    stack.push(Task::Char(')'));
                }
                let rb = priority(&nodes[*r as usize].kind) <= p;
                let lb = priority(&nodes[*l as usize].kind) <= p;
                stack.push(item(*r, rb));
                stack.push(Task::Char(' '));
                stack.push(Task::Str(binop_name(*op)));
                stack.push(Task::Char(' '));
                stack.push(item(*l, lb));
            }
            Kind::Not(a) => {
                out.push_str("!(");
                stack.push(Task::Char(')'));
                stack.push(item(*a, false));
            }
            Kind::IsUnknown(a) => {
                out.push('(');
                stack.push(Task::Str(") is unknown"));
                stack.push(item(*a, false));
            }
            Kind::Plus(a) | Kind::Minus(a) => {
                if brackets {
                    out.push('(');
                    stack.push(Task::Char(')'));
                }
                out.push(if matches!(node.kind, Kind::Plus(_)) {
                    '+'
                } else {
                    '-'
                });
                let ab = priority(&nodes[*a as usize].kind) <= priority(&node.kind);
                stack.push(item(*a, ab));
            }
            Kind::AnyArray => out.push_str("[*]"),
            Kind::AnyKey => {
                if in_key {
                    out.push('.');
                }
                out.push('*');
            }
            Kind::IndexArray(subs) => {
                out.push('[');
                stack.push(Task::Char(']'));
                for (i, (from, to)) in subs.iter().enumerate().rev() {
                    if let Some(to) = to {
                        stack.push(item(*to, false));
                        stack.push(Task::Str(" to "));
                    }
                    stack.push(item(*from, false));
                    if i > 0 {
                        stack.push(Task::Char(','));
                    }
                }
            }
            Kind::Any { first, last } => {
                if in_key {
                    out.push('.');
                }
                let (first, last) = (*first, *last);
                if first == 0 && last == u32::MAX {
                    out.push_str("**");
                } else if first == last {
                    if first == u32::MAX {
                        out.push_str("**{last}");
                    } else {
                        out.push_str(&format!("**{{{first}}}"));
                    }
                } else if first == u32::MAX {
                    out.push_str(&format!("**{{last to {last}}}"));
                } else if last == u32::MAX {
                    out.push_str(&format!("**{{{first} to last}}"));
                } else {
                    out.push_str(&format!("**{{{first} to {last}}}"));
                }
            }
            Kind::Key(k) => {
                if in_key {
                    out.push('.');
                }
                let _ = escape_json(out, k);
            }
            Kind::Current => out.push('@'),
            Kind::Root => out.push('$'),
            Kind::Variable(v) => {
                out.push('$');
                let _ = escape_json(out, v);
            }
            Kind::Filter(a) => {
                out.push_str("?(");
                stack.push(Task::Char(')'));
                stack.push(item(*a, false));
            }
            Kind::Exists(a) => {
                out.push_str("exists (");
                stack.push(Task::Char(')'));
                stack.push(item(*a, false));
            }
            Kind::Method(m) => {
                out.push('.');
                out.push_str(method_name(*m));
                out.push_str("()");
            }
            Kind::DtMethod(m, arg) => {
                out.push('.');
                out.push_str(dt_method_name(*m));
                out.push('(');
                stack.push(Task::Char(')'));
                if let Some(a) = arg {
                    stack.push(item(*a, false));
                }
            }
            Kind::Decimal(l, r) => {
                out.push_str(".decimal(");
                stack.push(Task::Char(')'));
                if let Some(r) = r {
                    stack.push(item(*r, false));
                    stack.push(Task::Char(','));
                }
                if let Some(l) = l {
                    stack.push(item(*l, false));
                }
            }
            Kind::Last => out.push_str("last"),
            Kind::LikeRegex {
                expr,
                pattern,
                flags,
                ..
            } => {
                if brackets {
                    out.push('(');
                    stack.push(Task::Char(')'));
                }
                let mut tail = String::from(" like_regex ");
                let _ = escape_json(&mut tail, pattern);
                if *flags != 0 {
                    tail.push_str(" flag \"");
                    for (bit, c) in [
                        (JSP_REGEX_ICASE, 'i'),
                        (JSP_REGEX_DOTALL, 's'),
                        (JSP_REGEX_MLINE, 'm'),
                        (JSP_REGEX_WSPACE, 'x'),
                        (JSP_REGEX_QUOTE, 'q'),
                    ] {
                        if flags & bit != 0 {
                            tail.push(c);
                        }
                    }
                    tail.push('"');
                }
                stack.push(Task::Owned(tail));
                let eb = priority(&nodes[*expr as usize].kind) <= priority(&node.kind);
                stack.push(item(*expr, eb));
            }
        }
    }
}
