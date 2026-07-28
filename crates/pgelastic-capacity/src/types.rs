//! Identifiers and the request discriminator.

use std::fmt;
use std::sync::Arc;

/// A tenant's name, unique within a pool.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TenantId(Arc<str>);

impl TenantId {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantId({:?})", &*self.0)
    }
}

impl From<&str> for TenantId {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for TenantId {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

macro_rules! opaque_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name(pub u64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }
    };
}

opaque_id!(ClientId, "client-");
opaque_id!(ServerId, "server-");
opaque_id!(TicketId, "ticket-");

/// What a client is asking a backend connection *for*.
///
/// A [`RequestKind::Cancel`] is the whole reason step 0 of the ladder exists: a
/// cancel that queues behind the query it is trying to cancel is a deadlock.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RequestKind {
    Normal,
    Cancel,
}
