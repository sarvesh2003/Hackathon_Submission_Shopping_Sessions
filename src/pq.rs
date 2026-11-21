//! Minimal hand-written Parquet writer: one row group, one PLAIN data page (v1) per column,
//! no compression, no statistics, no dictionary. Supports exactly what our two tables need:
//! REQUIRED/OPTIONAL BYTE_ARRAY (UTF8 string), REQUIRED INT32 (DATE), REQUIRED INT64.

use std::io::Write;

pub enum Column<'a> {
    /// UTF8 strings, all present: values concatenated in `data`, `ends[i]` = end offset of value i
    Str { name: &'a str, data: &'a [u8], ends: &'a [u32] },
    /// UTF8 strings, nullable: `present[i]` tells whether row i has a value; values (present only)
    /// as in `Str`
    OptStr { name: &'a str, data: &'a [u8], ends: &'a [u32], present: &'a [bool] },
    Date { name: &'a str, values: &'a [i32] },
    I64 { name: &'a str, values: &'a [i64] },
}

// parquet enums
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;
const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;
const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

// thrift compact types
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        *last = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, CT_I32);
        self.zigzag(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, CT_I64);
        self.zigzag(v);
    }
    fn binf(&mut self, id: i16, v: &[u8]) {
        self.field(id, CT_BINARY);
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn list_header(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, CT_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn begin_struct_field(&mut self, id: i16) {
        self.field(id, CT_STRUCT);
        self.last.push(0);
    }
    /// a struct as a list element (no field header)
    fn begin_struct(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    /// raw list element helpers
    fn elem_i32(&mut self, v: i32) {
        self.zigzag(v as i64);
    }
    fn elem_bin(&mut self, v: &[u8]) {
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }
}

impl<'a> Column<'a> {
    fn name(&self) -> &str {
        match self {
            Column::Str { name, .. }
            | Column::OptStr { name, .. }
            | Column::Date { name, .. }
            | Column::I64 { name, .. } => name,
        }
    }
    fn ptype(&self) -> i32 {
        match self {
            Column::Str { .. } | Column::OptStr { .. } => T_BYTE_ARRAY,
            Column::Date { .. } => T_INT32,
            Column::I64 { .. } => T_INT64,
        }
    }
}

/// Encodes the page body (def levels if optional, then PLAIN values).
fn page_body(col: &Column, out: &mut Vec<u8>) {
    match col {
        Column::Str { data, ends, .. } => plain_strings(data, ends, out),
        Column::OptStr { data, ends, present, .. } => {
            // definition levels: RLE/bit-packed hybrid, bit width 1, one bit-packed run,
            // prefixed by its byte length (data page v1)
            let groups = present.len().div_ceil(8);
            let mut lv = Vec::with_capacity(groups + 8);
            let mut h = ((groups as u64) << 1) | 1;
            while h >= 0x80 {
                lv.push((h as u8) | 0x80);
                h >>= 7;
            }
            lv.push(h as u8);
            for g in 0..groups {
                let mut b = 0u8;
                for k in 0..8 {
                    let i = g * 8 + k;
                    if i < present.len() && present[i] {
                        b |= 1 << k;
                    }
                }
                lv.push(b);
            }
            out.extend_from_slice(&(lv.len() as u32).to_le_bytes());
            out.extend_from_slice(&lv);
            plain_strings(data, ends, out);
        }
        Column::Date { values, .. } => {
            out.reserve(values.len() * 4);
            for v in values.iter() {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        Column::I64 { values, .. } => {
            out.reserve(values.len() * 8);
            for v in values.iter() {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    }
}

fn plain_strings(data: &[u8], ends: &[u32], out: &mut Vec<u8>) {
    out.reserve(data.len() + ends.len() * 4);
    let mut start = 0usize;
    for &e in ends {
        let e = e as usize;
        out.extend_from_slice(&((e - start) as u32).to_le_bytes());
        out.extend_from_slice(&data[start..e]);
        start = e;
    }
}

/// Writes a complete Parquet file with `rows` rows to `w`.
pub fn write_file<W: Write>(w: &mut W, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"PAR1");
    struct ChunkMeta {
        offset: i64,
        size: i64,
    }
    let mut metas = Vec::with_capacity(cols.len());
    if rows > 0 {
        let mut body = Vec::new();
        for col in cols {
            body.clear();
            page_body(col, &mut body);
            let mut h = Thrift::new();
            h.i32f(1, 0); // DATA_PAGE
            h.i32f(2, body.len() as i32);
            h.i32f(3, body.len() as i32);
            h.begin_struct_field(5); // DataPageHeader
            h.i32f(1, rows as i32);
            h.i32f(2, ENC_PLAIN);
            h.i32f(3, ENC_RLE);
            h.i32f(4, ENC_RLE);
            h.end_struct();
            h.buf.push(0); // end PageHeader
            let offset = file.len() as i64;
            file.extend_from_slice(&h.buf);
            file.extend_from_slice(&body);
            metas.push(ChunkMeta { offset, size: (h.buf.len() + body.len()) as i64 });
        }
    }

    // FileMetaData
    let mut m = Thrift::new();
    m.i32f(1, 1); // version
    m.list_header(2, CT_STRUCT, cols.len() + 1);
    m.begin_struct();
    m.binf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end_struct();
    for col in cols {
        m.begin_struct();
        m.i32f(1, col.ptype());
        let rep = if matches!(col, Column::OptStr { .. }) { REP_OPTIONAL } else { REP_REQUIRED };
        m.i32f(3, rep);
        m.binf(4, col.name().as_bytes());
        match col {
            Column::Str { .. } | Column::OptStr { .. } => {
                m.i32f(6, CT_UTF8);
                m.begin_struct_field(10); // LogicalType
                m.begin_struct_field(1); // STRING
                m.end_struct();
                m.end_struct();
            }
            Column::Date { .. } => {
                m.i32f(6, CT_DATE);
                m.begin_struct_field(10);
                m.begin_struct_field(6); // DATE
                m.end_struct();
                m.end_struct();
            }
            Column::I64 { .. } => {}
        }
        m.end_struct();
    }
    m.i64f(3, rows as i64);
    if rows > 0 {
        m.list_header(4, CT_STRUCT, 1);
        m.begin_struct(); // RowGroup
        m.list_header(1, CT_STRUCT, cols.len());
        let mut total = 0i64;
        for (col, meta) in cols.iter().zip(&metas) {
            total += meta.size;
            m.begin_struct(); // ColumnChunk
            m.i64f(2, meta.offset);
            m.begin_struct_field(3); // ColumnMetaData
            m.i32f(1, col.ptype());
            m.list_header(2, CT_I32, 2);
            m.elem_i32(ENC_PLAIN);
            m.elem_i32(ENC_RLE);
            m.list_header(3, CT_BINARY, 1);
            m.elem_bin(col.name().as_bytes());
            m.i32f(4, 0); // UNCOMPRESSED
            m.i64f(5, rows as i64);
            m.i64f(6, meta.size);
            m.i64f(7, meta.size);
            m.i64f(9, meta.offset);
            m.end_struct();
            m.end_struct();
        }
        m.i64f(2, total);
        m.i64f(3, rows as i64);
        m.end_struct();
    } else {
        m.list_header(4, CT_STRUCT, 0);
    }
    m.binf(6, b"shopping-sessions");
    m.buf.push(0); // end FileMetaData

    file.extend_from_slice(&m.buf);
    file.extend_from_slice(&(m.buf.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    w.write_all(&file)
}
