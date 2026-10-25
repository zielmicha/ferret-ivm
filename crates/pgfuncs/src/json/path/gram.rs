//! jsonpath parser: a port of src/backend/utils/adt/jsonpath_gram.y of
//! PostgreSQL 17 (Copyright (c) 2019-2024, PostgreSQL Global Development
//! Group; PostgreSQL License).
//!
//! The LALR(1) tables are bison's own (see `tables.rs`); this module
//! re-implements bison's `yyparse()` loop (including default reductions
//! without lookahead and the 10000-entry stack limit) and the semantic
//! actions, so syntax errors are detected at the same token and semantic
//! errors (numeric literals, regular expressions, ...) are raised in the same
//! order as in PostgreSQL.

use super::ast::*;
use super::scan::{Scanner, syntax_error};
use super::tables::*;
use crate::json::JsonNumber;
use crate::pattern::Regex;
use crate::{Error, Result, sqlstate};

/// Semantic values (YYSTYPE).
enum V {
    None,
    Str(Vec<u8>),
    Node(NodeId),
    List(Vec<NodeId>),
    /// index_elem: (from, to)
    Subscript(NodeId, Option<NodeId>),
    Subscripts(Vec<(NodeId, Option<NodeId>)>),
    BinOp(BinOp),
    Method(Method),
    Bool(bool),
    Int(i32),
}

impl V {
    fn node(&self) -> NodeId {
        match self {
            V::Node(n) => *n,
            _ => unreachable!("expected node"),
        }
    }
    fn opt_node(&self) -> Option<NodeId> {
        match self {
            V::Node(n) => Some(*n),
            V::None => None,
            _ => unreachable!("expected optional node"),
        }
    }
    fn take_str(&mut self) -> Vec<u8> {
        match std::mem::replace(self, V::None) {
            V::Str(s) => s,
            _ => unreachable!("expected string"),
        }
    }
    fn take_list(&mut self) -> Vec<NodeId> {
        match std::mem::replace(self, V::None) {
            V::List(l) => l,
            V::None => Vec::new(),
            _ => unreachable!("expected list"),
        }
    }
}

/// Result of a successful parse: `None` for an empty input.
pub(super) struct Parsed {
    pub lax: bool,
    pub nodes: Vec<Node>,
    pub root: NodeId,
}

/// YYMAXDEPTH
const YYMAXDEPTH: usize = 10000;
const YYEMPTY: i32 = -2;

fn bytes_to_string(b: Vec<u8>) -> String {
    String::from_utf8(b).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

struct Builder {
    nodes: Vec<Node>,
}

impl Builder {
    fn add(&mut self, kind: Kind) -> NodeId {
        self.nodes.push(Node { kind, next: None });
        (self.nodes.len() - 1) as NodeId
    }

    fn node_v(&mut self, kind: Kind) -> V {
        V::Node(self.add(kind))
    }

    /// makeItemNumeric(): numeric_in on the token text.
    fn numeric(&mut self, s: Vec<u8>) -> Result<V> {
        let s = bytes_to_string(s);
        let n = JsonNumber::parse(&s)?;
        Ok(self.node_v(Kind::Numeric(n)))
    }

    /// makeItemUnary(): folds `+literal` and `-literal`.
    fn unary(&mut self, plus: bool, a: NodeId) -> V {
        let node = &mut self.nodes[a as usize];
        if node.next.is_none()
            && let Kind::Numeric(n) = &node.kind
        {
            if !plus {
                node.kind = Kind::Numeric(n.negate());
            }
            return V::Node(a);
        }
        self.node_v(if plus { Kind::Plus(a) } else { Kind::Minus(a) })
    }

    /// makeItemList(): appends accessors to the end of the first item's
    /// chain.
    fn list(&mut self, list: Vec<NodeId>) -> NodeId {
        let head = list[0];
        if list.len() == 1 {
            return head;
        }
        let mut end = head;
        while let Some(n) = self.nodes[end as usize].next {
            end = n;
        }
        for &c in &list[1..] {
            self.nodes[end as usize].next = Some(c);
            end = c;
        }
        head
    }

    /// makeItemLikeRegex()
    fn like_regex(&mut self, expr: NodeId, pattern: Vec<u8>, flags: Option<Vec<u8>>) -> Result<V> {
        let mut bits = 0u32;
        if let Some(f) = &flags {
            let mut i = 0;
            while i < f.len() {
                match f[i] {
                    b'i' => bits |= JSP_REGEX_ICASE,
                    b's' => bits |= JSP_REGEX_DOTALL,
                    b'm' => bits |= JSP_REGEX_MLINE,
                    b'x' => bits |= JSP_REGEX_WSPACE,
                    b'q' => bits |= JSP_REGEX_QUOTE,
                    c => {
                        let len = utf8_char_len(c).min(f.len() - i);
                        return Err(Error::with_detail(
                            sqlstate::SYNTAX_ERROR,
                            "invalid input syntax for type jsonpath",
                            format!(
                                "Unrecognized flag character \"{}\" in LIKE_REGEX predicate.",
                                String::from_utf8_lossy(&f[i..i + len])
                            ),
                        ));
                    }
                }
                i += 1;
            }
        }
        let re_flags = regex_flags(bits)?;
        let pattern = bytes_to_string(pattern);
        let regex = Regex::new_cached(&pattern, re_flags)?;
        Ok(self.node_v(Kind::LikeRegex {
            expr,
            pattern,
            flags: bits,
            regex,
        }))
    }
}

/// pg_utf_mblen()
fn utf8_char_len(c: u8) -> usize {
    if c & 0x80 == 0 {
        1
    } else if c & 0xe0 == 0xc0 {
        2
    } else if c & 0xf0 == 0xe0 {
        3
    } else if c & 0xf8 == 0xf0 {
        4
    } else {
        1
    }
}

/// jspConvertRegexFlags(), expressed as regexp flag letters for
/// [`Regex::new`].
pub(super) fn regex_flags(xflags: u32) -> Result<&'static str> {
    let icase = xflags & JSP_REGEX_ICASE != 0;
    if xflags & JSP_REGEX_QUOTE != 0 {
        return Ok(if icase { "qi" } else { "q" });
    }
    if xflags & JSP_REGEX_WSPACE != 0 {
        return Err(Error::new(
            sqlstate::FEATURE_NOT_SUPPORTED,
            "XQuery \"x\" flag (expanded regular expressions) is not implemented",
        ));
    }
    let dotall = xflags & JSP_REGEX_DOTALL != 0;
    let mline = xflags & JSP_REGEX_MLINE != 0;
    // REG_ADVANCED, plus REG_NLSTOP unless dotall, plus REG_NLANCH if mline
    Ok(match (dotall, mline, icase) {
        (false, false, false) => "p",
        (false, false, true) => "pi",
        (true, false, false) => "",
        (true, false, true) => "i",
        (false, true, false) => "n",
        (false, true, true) => "ni",
        (true, true, false) => "w",
        (true, true, true) => "wi",
    })
}

/// parsejsonpath(): Ok(None) for an empty jsonpath.
pub(super) fn parse(input: &[u8]) -> Result<Option<Parsed>> {
    let mut sc = Scanner::new(input);
    let mut b = Builder { nodes: Vec::new() };
    let mut result: Option<(bool, NodeId)> = None;
    let mut states: Vec<i32> = Vec::with_capacity(64);
    let mut values: Vec<V> = Vec::with_capacity(64);
    let mut yychar = YYEMPTY;
    let mut yylval: Option<Vec<u8>> = None;
    let mut state: i32 = 0;
    values.push(V::None);
    'parse: loop {
        // yysetstate
        states.push(state);
        if states.len() >= YYMAXDEPTH {
            return Err(syntax_error("memory exhausted", sc.yytext()));
        }
        if state == YYFINAL {
            break;
        }
        // yybackup
        let rule: i32;
        let mut yyn = i32::from(YYPACT[state as usize]);
        'decide: {
            if yyn != YYPACT_NINF {
                if yychar == YYEMPTY {
                    let tok = sc.lex()?;
                    yychar = tok.code;
                    yylval = tok.str;
                }
                let yytoken = if yychar <= 0 {
                    yychar = 0;
                    0
                } else if yychar <= YYMAXUTOK {
                    i32::from(YYTRANSLATE[yychar as usize])
                } else {
                    2 // YYSYMBOL_YYUNDEF
                };
                yyn += yytoken;
                if (0..=YYLAST).contains(&yyn) && i32::from(YYCHECK[yyn as usize]) == yytoken {
                    yyn = i32::from(YYTABLE[yyn as usize]);
                    if yyn <= 0 {
                        rule = -yyn;
                        break 'decide;
                    }
                    // shift
                    state = yyn;
                    values.push(match yylval.take() {
                        Some(s) => V::Str(s),
                        None => V::None,
                    });
                    yychar = YYEMPTY;
                    continue 'parse;
                }
            }
            // yydefault
            rule = i32::from(YYDEFACT[state as usize]);
            if rule == 0 {
                return Err(syntax_error("syntax error", sc.yytext()));
            }
        }
        // yyreduce
        let len = YYR2[rule as usize] as usize;
        let base = values.len() - len;
        let val = action(rule, &mut values[base..], &mut b, &mut result)?;
        values.truncate(base);
        states.truncate(states.len() - len);
        values.push(val);
        let lhs = (i32::from(YYR1[rule as usize]) - YYNTOKENS) as usize;
        let top = *states.last().unwrap();
        let i = i32::from(YYPGOTO[lhs]) + top;
        state = if (0..=YYLAST).contains(&i) && i32::from(YYCHECK[i as usize]) == top {
            i32::from(YYTABLE[i as usize])
        } else {
            i32::from(YYDEFGOTO[lhs])
        };
    }
    Ok(result.map(|(lax, root)| Parsed {
        lax,
        nodes: b.nodes,
        root,
    }))
}

/// Semantic actions; `v` holds the right-hand side values ($1 .. $n).
fn action(
    rule: i32,
    v: &mut [V],
    b: &mut Builder,
    result: &mut Option<(bool, NodeId)>,
) -> Result<V> {
    use BinOp::*;
    Ok(match rule {
        // result: mode expr_or_predicate
        2 => {
            let lax = matches!(v[0], V::Bool(true));
            *result = Some((lax, v[1].node()));
            V::None
        }
        // result: %empty
        3 => {
            *result = None;
            V::None
        }
        // expr_or_predicate
        4 | 5 => V::Node(v[0].node()),
        // mode
        6 => V::Bool(false),
        7 | 8 => V::Bool(true),
        // scalar_value
        9 => {
            let s = bytes_to_string(v[0].take_str());
            b.node_v(Kind::String(s))
        }
        10 => b.node_v(Kind::Null),
        11 => b.node_v(Kind::Bool(true)),
        12 => b.node_v(Kind::Bool(false)),
        13 | 14 => b.numeric(v[0].take_str())?,
        15 => {
            let s = bytes_to_string(v[0].take_str());
            b.node_v(Kind::Variable(s))
        }
        // comp_op
        16 => V::BinOp(Equal),
        17 => V::BinOp(NotEqual),
        18 => V::BinOp(Less),
        19 => V::BinOp(Greater),
        20 => V::BinOp(LessOrEqual),
        21 => V::BinOp(GreaterOrEqual),
        // delimited_predicate: '(' predicate ')'
        22 => V::Node(v[1].node()),
        // EXISTS_P '(' expr ')'
        23 => b.node_v(Kind::Exists(v[2].node())),
        // predicate: delimited_predicate
        24 => V::Node(v[0].node()),
        // expr comp_op expr
        25 => {
            let op = match v[1] {
                V::BinOp(op) => op,
                _ => unreachable!(),
            };
            b.node_v(Kind::Binary(op, v[0].node(), v[2].node()))
        }
        26 => b.node_v(Kind::Binary(And, v[0].node(), v[2].node())),
        27 => b.node_v(Kind::Binary(Or, v[0].node(), v[2].node())),
        // NOT_P delimited_predicate
        28 => b.node_v(Kind::Not(v[1].node())),
        // '(' predicate ')' IS_P UNKNOWN_P
        29 => b.node_v(Kind::IsUnknown(v[1].node())),
        // expr STARTS_P WITH_P starts_with_initial
        30 => b.node_v(Kind::Binary(StartsWith, v[0].node(), v[3].node())),
        // expr LIKE_REGEX_P STRING_P
        31 => {
            let pattern = v[2].take_str();
            b.like_regex(v[0].node(), pattern, None)?
        }
        // expr LIKE_REGEX_P STRING_P FLAG_P STRING_P
        32 => {
            let pattern = v[2].take_str();
            let flags = v[4].take_str();
            b.like_regex(v[0].node(), pattern, Some(flags))?
        }
        // starts_with_initial
        33 => {
            let s = bytes_to_string(v[0].take_str());
            b.node_v(Kind::String(s))
        }
        34 => {
            let s = bytes_to_string(v[0].take_str());
            b.node_v(Kind::Variable(s))
        }
        // path_primary
        35 => V::Node(v[0].node()),
        36 => b.node_v(Kind::Root),
        37 => b.node_v(Kind::Current),
        38 => b.node_v(Kind::Last),
        // accessor_expr: path_primary
        39 => V::List(vec![v[0].node()]),
        // '(' expr ')' accessor_op  |  '(' predicate ')' accessor_op
        40 | 41 => V::List(vec![v[1].node(), v[3].node()]),
        // accessor_expr accessor_op
        42 => {
            let mut l = v[0].take_list();
            l.push(v[1].node());
            V::List(l)
        }
        // expr: accessor_expr
        43 => {
            let l = v[0].take_list();
            V::Node(b.list(l))
        }
        // '(' expr ')'
        44 => V::Node(v[1].node()),
        // '+' expr  |  '-' expr
        45 => b.unary(true, v[1].node()),
        46 => b.unary(false, v[1].node()),
        47 => b.node_v(Kind::Binary(Add, v[0].node(), v[2].node())),
        48 => b.node_v(Kind::Binary(Sub, v[0].node(), v[2].node())),
        49 => b.node_v(Kind::Binary(Mul, v[0].node(), v[2].node())),
        50 => b.node_v(Kind::Binary(Div, v[0].node(), v[2].node())),
        51 => b.node_v(Kind::Binary(Mod, v[0].node(), v[2].node())),
        // index_elem
        52 => V::Subscript(v[0].node(), None),
        53 => V::Subscript(v[0].node(), Some(v[2].node())),
        // index_list
        54 => match v[0] {
            V::Subscript(f, t) => V::Subscripts(vec![(f, t)]),
            _ => unreachable!(),
        },
        55 => {
            let mut l = match std::mem::replace(&mut v[0], V::None) {
                V::Subscripts(l) => l,
                _ => unreachable!(),
            };
            match v[2] {
                V::Subscript(f, t) => l.push((f, t)),
                _ => unreachable!(),
            }
            V::Subscripts(l)
        }
        // array_accessor
        56 => b.node_v(Kind::AnyArray),
        57 => match std::mem::replace(&mut v[1], V::None) {
            V::Subscripts(l) => b.node_v(Kind::IndexArray(l)),
            _ => unreachable!(),
        },
        // any_level: INT_P  (pg_strtoint32)
        58 => {
            let s = bytes_to_string(v[0].take_str());
            V::Int(crate::text::parse_int4(&s)?)
        }
        // any_level: LAST_P
        59 => V::Int(-1),
        // any_path
        60 => b.node_v(make_any(0, -1)),
        61 => {
            let l = int_of(&v[2]);
            b.node_v(make_any(l, l))
        }
        62 => {
            let (f, l) = (int_of(&v[2]), int_of(&v[4]));
            b.node_v(make_any(f, l))
        }
        // accessor_op
        63 => V::Node(v[1].node()),
        64 => b.node_v(Kind::AnyKey),
        65 => V::Node(v[0].node()),
        66 => V::Node(v[1].node()),
        67 => match v[1] {
            V::Method(m) => b.node_v(Kind::Method(m)),
            _ => unreachable!(),
        },
        68 => b.node_v(Kind::Filter(v[2].node())),
        // '.' DECIMAL_P '(' opt_csv_list ')'
        69 => {
            let l = v[3].take_list();
            match l.len() {
                0 => b.node_v(Kind::Decimal(None, None)),
                1 => b.node_v(Kind::Decimal(Some(l[0]), None)),
                2 => b.node_v(Kind::Decimal(Some(l[0]), Some(l[1]))),
                _ => {
                    return Err(Error::with_detail(
                        sqlstate::SYNTAX_ERROR,
                        "invalid input syntax for type jsonpath",
                        ".decimal() can only have an optional precision[,scale].",
                    ));
                }
            }
        }
        70 => b.node_v(Kind::DtMethod(DtMethod::Datetime, v[3].opt_node())),
        71 => b.node_v(Kind::DtMethod(DtMethod::Time, v[3].opt_node())),
        72 => b.node_v(Kind::DtMethod(DtMethod::TimeTz, v[3].opt_node())),
        73 => b.node_v(Kind::DtMethod(DtMethod::Timestamp, v[3].opt_node())),
        74 => b.node_v(Kind::DtMethod(DtMethod::TimestampTz, v[3].opt_node())),
        // csv_elem
        75 => b.numeric(v[0].take_str())?,
        76 | 77 => {
            let n = b.numeric(v[1].take_str())?.node();
            b.unary(rule == 76, n)
        }
        // csv_list
        78 => V::List(vec![v[0].node()]),
        79 => {
            let mut l = v[0].take_list();
            l.push(v[2].node());
            V::List(l)
        }
        // opt_csv_list
        80 => V::List(v[0].take_list()),
        81 => V::List(Vec::new()),
        // datetime_precision: INT_P
        82 => b.numeric(v[0].take_str())?,
        // opt_datetime_precision
        83 => V::Node(v[0].node()),
        84 => V::None,
        // datetime_template: STRING_P
        85 => {
            let s = bytes_to_string(v[0].take_str());
            b.node_v(Kind::String(s))
        }
        // opt_datetime_template
        86 => V::Node(v[0].node()),
        87 => V::None,
        // key: key_name
        88 => {
            let s = bytes_to_string(v[0].take_str());
            b.node_v(Kind::Key(s))
        }
        // key_name: any keyword token, IDENT_P or STRING_P  ($$ = $1)
        89..=123 => V::Str(v[0].take_str()),
        // method
        124 => V::Method(Method::Abs),
        125 => V::Method(Method::Size),
        126 => V::Method(Method::Type),
        127 => V::Method(Method::Floor),
        128 => V::Method(Method::Double),
        129 => V::Method(Method::Ceiling),
        130 => V::Method(Method::KeyValue),
        131 => V::Method(Method::Bigint),
        132 => V::Method(Method::Boolean),
        133 => V::Method(Method::Date),
        134 => V::Method(Method::Integer),
        135 => V::Method(Method::Number),
        136 => V::Method(Method::StringFunc),
        _ => unreachable!("unknown jsonpath grammar rule {rule}"),
    })
}

fn int_of(v: &V) -> i32 {
    match v {
        V::Int(i) => *i,
        _ => unreachable!(),
    }
}

/// makeAny()
fn make_any(first: i32, last: i32) -> Kind {
    Kind::Any {
        first: if first >= 0 { first as u32 } else { u32::MAX },
        last: if last >= 0 { last as u32 } else { u32::MAX },
    }
}
