//! An optimized implementation of reading and writing ISO-flavored WKB-encoded geometries.

mod api;
pub(crate) mod reader;
pub(crate) mod writer;

pub use api::{from_wkb, to_wkb, FromWKB};
