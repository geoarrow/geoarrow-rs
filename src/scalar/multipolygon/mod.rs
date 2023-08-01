#[cfg(feature = "geos")]
mod geos;
mod iterator;
mod scalar;

pub use iterator::MultiPolygonIterator;
pub use scalar::MultiPolygon;
