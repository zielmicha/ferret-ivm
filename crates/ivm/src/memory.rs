//! Sizes in memory of the data the server holds. Everything is measured in
//! bytes of memory — the sizes of the structs and of the heap blocks they
//! own ([`heap_block`]) — not in serialized bytes or in counts, so that a
//! limit means the same for narrow and for wide rows.

use crate::types::Datum;
use std::mem::{size_of, size_of_val};

/// Bytes a heap allocation of `n` bytes occupies. Allocators hand out small
/// blocks in size classes: glibc adds an 8-byte header and rounds to 16
/// bytes, mimalloc rounds to classes up to 12.5% apart. Rounding the size
/// plus a word up to 16 bytes is close to both.
#[inline]
pub fn heap_block(n: usize) -> usize {
    if n == 0 { 0 } else { (n + 8 + 15) & !15 }
}

/// Heap bytes owned by a datum (beyond its own size). Reference-counted
/// payloads are counted in full, also when they are shared.
pub fn datum_heap(d: &Datum) -> usize {
    // an Arc's block holds two counters before the value
    const ARC: usize = 16;
    match d {
        Datum::Numeric(n) => heap_block(ARC + size_of_val(&**n)),
        Datum::Text(s) | Datum::Json(s) => heap_block(ARC + s.len()),
        Datum::Bytea(b) => heap_block(ARC + b.len()),
        Datum::Jsonb(j) => heap_block(ARC + size_of_val(&**j)) + jsonb_heap(j),
        Datum::Array(a) => {
            heap_block(ARC + size_of_val(&**a))
                + heap_block(a.dims.capacity() * size_of::<(i32, i32)>())
                + heap_block(a.elems.capacity() * size_of::<Datum>())
                + a.elems.iter().map(datum_heap).sum::<usize>()
        }
        _ => 0,
    }
}

fn jsonb_heap(j: &pgfuncs::json::Jsonb) -> usize {
    use pgfuncs::json::Jsonb;
    match j {
        Jsonb::Null | Jsonb::Bool(_) => 0,
        Jsonb::Number(n) => heap_block(n.as_str().len()),
        Jsonb::String(s) => heap_block(s.capacity()),
        Jsonb::Array(v) => heap_block(v.capacity() * size_of::<Jsonb>()) + v.iter().map(jsonb_heap).sum::<usize>(),
        Jsonb::Object(v) => {
            heap_block(v.capacity() * size_of::<(String, Jsonb)>())
                + v.iter().map(|(k, x)| heap_block(k.capacity()) + jsonb_heap(x)).sum::<usize>()
        }
    }
}
