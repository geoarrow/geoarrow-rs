//! Contains the [`WKBArray`] and [`MutableWKBArray`] for arrays of WKB-encoded
//! geometries.

pub use array::WKBArray;
pub use mutable::MutableWKBArray;

mod array;
mod iterator;
mod mutable;
