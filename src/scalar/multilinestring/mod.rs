#[cfg(feature = "geos")]
mod geos;
mod iterator;
mod scalar;

pub use iterator::MultiLineStringIterator;
pub use scalar::MultiLineString;
