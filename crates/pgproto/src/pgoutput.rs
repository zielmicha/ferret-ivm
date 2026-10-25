//! Decoding of pgoutput logical replication messages (protocol version 1,
//! plus logical decoding messages enabled by the `messages` option).
//!
//! Streaming of in-progress transactions (protocol version >= 2) and
//! two-phase messages are not supported: those formats carry extra fields
//! whose presence depends on stream state.

use crate::wire::Reader;
use crate::{Error, Lsn, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalMessage {
    Begin {
        /// LSN of the commit record (`Commit::commit_lsn`).
        final_lsn: Lsn,
        /// Microseconds since 2000-01-01 00:00:00 UTC.
        commit_time: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        /// LSN of the commit record.
        commit_lsn: Lsn,
        /// End LSN of the commit record: the position to confirm (flush)
        /// once the transaction has been durably processed.
        end_lsn: Lsn,
        commit_time: i64,
    },
    Origin {
        commit_lsn: Lsn,
        name: String,
    },
    Relation(Relation),
    Type {
        id: u32,
        namespace: String,
        name: String,
    },
    Insert {
        relation_id: u32,
        new: Tuple,
    },
    Update {
        relation_id: u32,
        /// Old key ('K', replica identity columns only; other columns are
        /// `Null`) or old full row ('O', REPLICA IDENTITY FULL). `None` when
        /// the key did not change and the identity is not FULL.
        old: Option<Tuple>,
        /// `true` for 'K', `false` for 'O'.
        old_is_key: bool,
        new: Tuple,
    },
    Delete {
        relation_id: u32,
        old: Tuple,
        old_is_key: bool,
    },
    Truncate {
        /// Bit 1: CASCADE, bit 2: RESTART IDENTITY.
        options: u8,
        relation_ids: Vec<u32>,
    },
    Message {
        transactional: bool,
        lsn: Lsn,
        prefix: String,
        content: Vec<u8>,
    },
}

/// Truncate option flag: CASCADE.
pub const TRUNCATE_CASCADE: u8 = 1;
/// Truncate option flag: RESTART IDENTITY.
pub const TRUNCATE_RESTART_IDENTITY: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    /// Empty string for `pg_catalog`.
    pub namespace: String,
    pub name: String,
    /// `b'd'` default, `b'n'` nothing, `b'f'` full, `b'i'` index.
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationColumn {
    /// Part of the replica identity (for FULL: every column).
    pub is_key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

pub type Tuple = Vec<TupleValue>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// A TOASTed value that was not changed (and is not part of the old
    /// key); the actual value is not sent.
    UnchangedToast,
    /// Text format value (type output function, client encoding).
    Text(Vec<u8>),
    /// Binary format value (only with the `binary` option).
    Binary(Vec<u8>),
}

impl TupleValue {
    /// The raw bytes of a `Text` or `Binary` value.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            TupleValue::Text(b) | TupleValue::Binary(b) => Some(b),
            _ => None,
        }
    }

    /// The value of a `Text` value as UTF-8, if valid.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            TupleValue::Text(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }
}

fn read_tuple(r: &mut Reader<'_>) -> Result<Tuple> {
    let n = r.u16()? as usize;
    let mut t = Vec::with_capacity(n);
    for _ in 0..n {
        let v = match r.u8()? {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::UnchangedToast,
            kind @ (b't' | b'b') => {
                let len = r.i32()?;
                if len < 0 {
                    return Err(Error::protocol(format!(
                        "pgoutput: negative value length {len}"
                    )));
                }
                let data = r.bytes(len as usize)?.to_vec();
                if kind == b't' {
                    TupleValue::Text(data)
                } else {
                    TupleValue::Binary(data)
                }
            }
            k => {
                return Err(Error::protocol(format!(
                    "pgoutput: unknown tuple column kind {:?}",
                    k as char
                )));
            }
        };
        t.push(v);
    }
    Ok(t)
}

fn expect_new_tuple(r: &mut Reader<'_>, kind: u8) -> Result<Tuple> {
    if kind != b'N' {
        return Err(Error::protocol(format!(
            "pgoutput: expected 'N' tuple marker, got {:?}",
            kind as char
        )));
    }
    read_tuple(r)
}

/// Parses one pgoutput message (the payload of an XLogData message).
pub fn parse(data: &[u8]) -> Result<LogicalMessage> {
    let mut r = Reader::new(data);
    let tag = r.u8()?;
    let msg = match tag {
        b'B' => LogicalMessage::Begin {
            final_lsn: Lsn(r.u64()?),
            commit_time: r.i64()?,
            xid: r.u32()?,
        },
        b'C' => LogicalMessage::Commit {
            flags: r.u8()?,
            commit_lsn: Lsn(r.u64()?),
            end_lsn: Lsn(r.u64()?),
            commit_time: r.i64()?,
        },
        b'O' => LogicalMessage::Origin {
            commit_lsn: Lsn(r.u64()?),
            name: r.cstr()?,
        },
        b'R' => {
            let id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let replica_identity = r.u8()?;
            let n = r.u16()? as usize;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                let flags = r.u8()?;
                columns.push(RelationColumn {
                    is_key: flags & 1 != 0,
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    type_modifier: r.i32()?,
                });
            }
            LogicalMessage::Relation(Relation {
                id,
                namespace,
                name,
                replica_identity,
                columns,
            })
        }
        b'Y' => LogicalMessage::Type {
            id: r.u32()?,
            namespace: r.cstr()?,
            name: r.cstr()?,
        },
        b'I' => {
            let relation_id = r.u32()?;
            let kind = r.u8()?;
            LogicalMessage::Insert {
                relation_id,
                new: expect_new_tuple(&mut r, kind)?,
            }
        }
        b'U' => {
            let relation_id = r.u32()?;
            let mut kind = r.u8()?;
            let mut old = None;
            let mut old_is_key = false;
            if kind == b'K' || kind == b'O' {
                old_is_key = kind == b'K';
                old = Some(read_tuple(&mut r)?);
                kind = r.u8()?;
            }
            LogicalMessage::Update {
                relation_id,
                old,
                old_is_key,
                new: expect_new_tuple(&mut r, kind)?,
            }
        }
        b'D' => {
            let relation_id = r.u32()?;
            let kind = r.u8()?;
            if kind != b'K' && kind != b'O' {
                return Err(Error::protocol(format!(
                    "pgoutput: expected 'K' or 'O' in Delete, got {:?}",
                    kind as char
                )));
            }
            LogicalMessage::Delete {
                relation_id,
                old: read_tuple(&mut r)?,
                old_is_key: kind == b'K',
            }
        }
        b'T' => {
            let n = r.u32()? as usize;
            let options = r.u8()?;
            if n > r.remaining() / 4 {
                return Err(Error::protocol(
                    "pgoutput: Truncate relation count too large",
                ));
            }
            let mut relation_ids = Vec::with_capacity(n);
            for _ in 0..n {
                relation_ids.push(r.u32()?);
            }
            LogicalMessage::Truncate {
                options,
                relation_ids,
            }
        }
        b'M' => {
            let flags = r.u8()?;
            let lsn = Lsn(r.u64()?);
            let prefix = r.cstr()?;
            let len = r.u32()? as usize;
            LogicalMessage::Message {
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                content: r.bytes(len)?.to_vec(),
            }
        }
        other => {
            return Err(Error::protocol(format!(
                "pgoutput: unsupported message type {:?} (0x{other:02x})",
                other as char
            )));
        }
    };
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple_bytes(vals: &[Option<&[u8]>]) -> Vec<u8> {
        let mut b = (vals.len() as u16).to_be_bytes().to_vec();
        for v in vals {
            match v {
                None => b.push(b'n'),
                Some(d) => {
                    b.push(b't');
                    b.extend_from_slice(&(d.len() as i32).to_be_bytes());
                    b.extend_from_slice(d);
                }
            }
        }
        b
    }

    #[test]
    fn begin_commit() {
        let mut b = vec![b'B'];
        b.extend_from_slice(&0x1_0000_0010u64.to_be_bytes());
        b.extend_from_slice(&12345i64.to_be_bytes());
        b.extend_from_slice(&777u32.to_be_bytes());
        assert_eq!(
            parse(&b).unwrap(),
            LogicalMessage::Begin {
                final_lsn: Lsn(0x1_0000_0010),
                commit_time: 12345,
                xid: 777
            }
        );
        let mut c = vec![b'C', 0];
        c.extend_from_slice(&10u64.to_be_bytes());
        c.extend_from_slice(&20u64.to_be_bytes());
        c.extend_from_slice(&(-5i64).to_be_bytes());
        assert_eq!(
            parse(&c).unwrap(),
            LogicalMessage::Commit {
                flags: 0,
                commit_lsn: Lsn(10),
                end_lsn: Lsn(20),
                commit_time: -5
            }
        );
        assert!(parse(&c[..c.len() - 1]).is_err());
    }

    #[test]
    fn relation_and_update() {
        let mut b = vec![b'R'];
        b.extend_from_slice(&16384u32.to_be_bytes());
        b.extend_from_slice(b"public\0t\0");
        b.push(b'd');
        b.extend_from_slice(&2u16.to_be_bytes());
        b.push(1);
        b.extend_from_slice(b"id\0");
        b.extend_from_slice(&23u32.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.push(0);
        b.extend_from_slice(b"v\0");
        b.extend_from_slice(&1043u32.to_be_bytes());
        b.extend_from_slice(&14i32.to_be_bytes());
        let m = parse(&b).unwrap();
        assert_eq!(
            m,
            LogicalMessage::Relation(Relation {
                id: 16384,
                namespace: "public".into(),
                name: "t".into(),
                replica_identity: b'd',
                columns: vec![
                    RelationColumn {
                        is_key: true,
                        name: "id".into(),
                        type_oid: 23,
                        type_modifier: -1
                    },
                    RelationColumn {
                        is_key: false,
                        name: "v".into(),
                        type_oid: 1043,
                        type_modifier: 14
                    },
                ]
            })
        );

        let mut u = vec![b'U'];
        u.extend_from_slice(&16384u32.to_be_bytes());
        u.push(b'K');
        u.extend_from_slice(&tuple_bytes(&[Some(b"1"), None]));
        u.push(b'N');
        let mut t = tuple_bytes(&[Some(b"2"), None]);
        t.push(b'u');
        t[1] = 3; // three columns
        u.extend_from_slice(&t);
        assert_eq!(
            parse(&u).unwrap(),
            LogicalMessage::Update {
                relation_id: 16384,
                old: Some(vec![TupleValue::Text(b"1".to_vec()), TupleValue::Null]),
                old_is_key: true,
                new: vec![
                    TupleValue::Text(b"2".to_vec()),
                    TupleValue::Null,
                    TupleValue::UnchangedToast
                ],
            }
        );

        let mut u2 = vec![b'U'];
        u2.extend_from_slice(&5u32.to_be_bytes());
        u2.push(b'N');
        u2.extend_from_slice(&tuple_bytes(&[Some(b"x")]));
        assert_eq!(
            parse(&u2).unwrap(),
            LogicalMessage::Update {
                relation_id: 5,
                old: None,
                old_is_key: false,
                new: vec![TupleValue::Text(b"x".to_vec())]
            }
        );
    }

    #[test]
    fn truncate_message_unknown() {
        let mut b = vec![b'T'];
        b.extend_from_slice(&2u32.to_be_bytes());
        b.push(3);
        b.extend_from_slice(&7u32.to_be_bytes());
        b.extend_from_slice(&8u32.to_be_bytes());
        assert_eq!(
            parse(&b).unwrap(),
            LogicalMessage::Truncate {
                options: 3,
                relation_ids: vec![7, 8]
            }
        );
        let mut m = vec![b'M', 1];
        m.extend_from_slice(&99u64.to_be_bytes());
        m.extend_from_slice(b"ivm\0");
        m.extend_from_slice(&5u32.to_be_bytes());
        m.extend_from_slice(b"hello");
        assert_eq!(
            parse(&m).unwrap(),
            LogicalMessage::Message {
                transactional: true,
                lsn: Lsn(99),
                prefix: "ivm".into(),
                content: b"hello".to_vec()
            }
        );
        assert!(parse(b"Z").is_err());
        assert!(parse(b"").is_err());
    }
}
