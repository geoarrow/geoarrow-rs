//! Contains vectorized algorithms implemented on and returning GeoArrow arrays.

pub mod broadcasting;
pub mod geo;
#[cfg(feature = "geodesy")]
pub mod geodesy;
pub mod native;
