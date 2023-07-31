//! Contains the [`MultiPolygonArray`] and [`MutableMultiPolygonArray`] for arrays of MultiPolygon
//! geometries.

#[cfg(feature = "geozero")]
pub use self::geozero::ToGeoArrowMultiPolygonArray;
pub use array::MultiPolygonArray;
pub use iterator::{MultiPolygonArrayValuesIter, MultiPolygonIterator};
pub use mutable::MutableMultiPolygonArray;

mod array;
#[cfg(feature = "geozero")]
mod geozero;
pub mod iterator;
mod mutable;
