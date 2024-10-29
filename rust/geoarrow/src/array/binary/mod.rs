//! Contains the [`WKBArray`] and [`WKBBuilder`] for arrays of WKB-encoded
//! geometries.

pub use array::WKBArray;
pub use builder::WKBBuilder;
pub use capacity::WKBCapacity;

mod array;
mod builder;
mod capacity;
