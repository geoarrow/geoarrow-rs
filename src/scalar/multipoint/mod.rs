#[cfg(feature = "geos")]
mod geos;
mod iterator;
mod scalar;

pub use iterator::MultiPointIterator;
pub use scalar::MultiPoint;
