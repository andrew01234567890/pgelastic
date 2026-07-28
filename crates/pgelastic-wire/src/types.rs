use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{Reader, put_cstr};
use crate::error::WireError;

/// The transaction-status byte carried by `ReadyForQuery` (`'Z'`).
///
/// This is the pooling release boundary. `Idle` is the *only* status under
/// which a backend link may be considered for check-in; SQL text is never
/// parsed to find transaction boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionStatus {
    /// `'I'` — not in a transaction block.
    Idle,
    /// `'T'` — in a transaction block.
    Transaction,
    /// `'E'` — in a failed transaction block; only `ROLLBACK` will be accepted.
    Failed,
}

impl TransactionStatus {
    pub fn as_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::Transaction => b'T',
            Self::Failed => b'E',
        }
    }

    pub fn from_byte(byte: u8) -> Result<Self, WireError> {
        match byte {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::Transaction),
            b'E' => Ok(Self::Failed),
            other => Err(WireError::InvalidTransactionStatus(other)),
        }
    }

    /// Whether the link may be *considered* for check-in.
    ///
    /// Necessary, never sufficient: the full gate additionally requires an empty
    /// outstanding-request queue, no client COPY-in, no in-flight cancel and no
    /// close-needed flag.
    pub fn is_releasable(self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Text or binary encoding of a parameter or result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Format {
    #[default]
    Text,
    Binary,
}

impl Format {
    pub fn as_i16(self) -> i16 {
        match self {
            Self::Text => 0,
            Self::Binary => 1,
        }
    }

    pub fn from_i16(code: i16) -> Result<Self, WireError> {
        match code {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            other => Err(WireError::InvalidFormat(other)),
        }
    }
}

/// What a `Describe` or `Close` refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Target {
    /// `'S'` — a prepared statement.
    Statement,
    /// `'P'` — a portal.
    Portal,
}

impl Target {
    pub fn as_byte(self) -> u8 {
        match self {
            Self::Statement => b'S',
            Self::Portal => b'P',
        }
    }

    pub fn from_byte(byte: u8) -> Result<Self, WireError> {
        match byte {
            b'S' => Ok(Self::Statement),
            b'P' => Ok(Self::Portal),
            other => Err(WireError::InvalidTarget(other)),
        }
    }
}

/// A backend cancellation key.
///
/// Never a `u32`: protocol 3.2 lengthened the key to a variable-width byte
/// string of 4 to 256 bytes, and pgelastic mints structured keys of its own
/// that carry a routing id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CancelKey(Bytes);

impl CancelKey {
    pub const MIN_LEN: usize = 4;
    pub const MAX_LEN: usize = 256;

    pub fn new(bytes: Bytes) -> Result<Self, WireError> {
        if !(Self::MIN_LEN..=Self::MAX_LEN).contains(&bytes.len()) {
            return Err(WireError::InvalidCancelKeyLength(bytes.len()));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }
}

/// One column of a `RowDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: Bytes,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: Format,
}

impl FieldDescription {
    pub(crate) fn decode(r: &mut Reader) -> Result<Self, WireError> {
        Ok(Self {
            name: r.cstr()?,
            table_oid: r.u32()?,
            column_id: r.i16()?,
            type_oid: r.u32()?,
            type_size: r.i16()?,
            type_modifier: r.i32()?,
            format: Format::from_i16(r.i16()?)?,
        })
    }

    pub(crate) fn encode(&self, dst: &mut BytesMut) {
        put_cstr(dst, &self.name);
        dst.put_u32(self.table_oid);
        dst.put_i16(self.column_id);
        dst.put_u32(self.type_oid);
        dst.put_i16(self.type_size);
        dst.put_i32(self.type_modifier);
        dst.put_i16(self.format.as_i16());
    }
}

/// Field type bytes used by `ErrorResponse` and `NoticeResponse`.
pub mod field {
    pub const SEVERITY: u8 = b'S';
    pub const SEVERITY_NONLOCALIZED: u8 = b'V';
    pub const CODE: u8 = b'C';
    pub const MESSAGE: u8 = b'M';
    pub const DETAIL: u8 = b'D';
    pub const HINT: u8 = b'H';
    pub const POSITION: u8 = b'P';
    pub const INTERNAL_POSITION: u8 = b'p';
    pub const INTERNAL_QUERY: u8 = b'q';
    pub const WHERE: u8 = b'W';
    pub const SCHEMA: u8 = b's';
    pub const TABLE: u8 = b't';
    pub const COLUMN: u8 = b'c';
    pub const DATA_TYPE: u8 = b'd';
    pub const CONSTRAINT: u8 = b'n';
    pub const FILE: u8 = b'F';
    pub const LINE: u8 = b'L';
    pub const ROUTINE: u8 = b'R';
}

/// The structured field list shared by `ErrorResponse` and `NoticeResponse`.
///
/// Fields are kept in wire order, duplicates and all, and unknown field types
/// are preserved verbatim — a proxy that drops them corrupts the error a client
/// sees.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fields(Vec<(u8, Bytes)>);

impl Fields {
    pub fn new(fields: Vec<(u8, Bytes)>) -> Self {
        Self(fields)
    }

    pub fn as_slice(&self) -> &[(u8, Bytes)] {
        &self.0
    }

    pub fn push(&mut self, kind: u8, value: Bytes) {
        self.0.push((kind, value));
    }

    pub fn get(&self, kind: u8) -> Option<&Bytes> {
        self.0.iter().find(|(k, _)| *k == kind).map(|(_, v)| v)
    }

    /// The five-character `SQLSTATE`, field `'C'`.
    pub fn sqlstate(&self) -> Option<&Bytes> {
        self.get(field::CODE)
    }

    pub fn severity(&self) -> Option<&Bytes> {
        self.get(field::SEVERITY_NONLOCALIZED)
            .or_else(|| self.get(field::SEVERITY))
    }

    pub fn message(&self) -> Option<&Bytes> {
        self.get(field::MESSAGE)
    }

    pub(crate) fn decode(r: &mut Reader) -> Result<Self, WireError> {
        let mut fields = Vec::new();
        loop {
            let kind = r.u8()?;
            if kind == 0 {
                break;
            }
            fields.push((kind, r.cstr()?));
        }
        Ok(Self(fields))
    }

    pub(crate) fn encode(&self, dst: &mut BytesMut) {
        for (kind, value) in &self.0 {
            dst.put_u8(*kind);
            put_cstr(dst, value);
        }
        dst.put_u8(0);
    }
}

impl FromIterator<(u8, Bytes)> for Fields {
    fn from_iter<T: IntoIterator<Item = (u8, Bytes)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
