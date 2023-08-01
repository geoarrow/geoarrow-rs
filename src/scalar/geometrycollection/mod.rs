#[cfg(feature = "geos")]
mod geos;
mod iterator;
mod scalar;

pub use iterator::GeometryCollectionIterator;
pub use scalar::GeometryCollection;
