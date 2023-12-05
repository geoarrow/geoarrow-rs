//! Contains the [`WKBArray`] and [`WKBBuilder`] for arrays of WKB-encoded
//! geometries.

pub use array::WKBArray;
pub use builder::WKBBuilder;

mod array;
mod iterator;
mod builder;
