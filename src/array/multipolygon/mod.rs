//! Contains the [`MultiPolygonArray`] and [`MutableMultiPolygonArray`] for arrays of MultiPolygon
//! geometries.

pub use array::MultiPolygonArray;
pub use iterator::{MultiPolygonArrayValuesIter, MultiPolygonIterator};
pub use mutable::MutableMultiPolygonArray;

mod array;
pub mod iterator;
mod mutable;
