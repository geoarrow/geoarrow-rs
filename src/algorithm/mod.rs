//! Vectorized algorithms implemented on and returning GeoArrow arrays.

pub mod broadcasting;
pub mod geo;
#[cfg(feature = "geodesy")]
pub mod geodesy;
#[cfg(feature = "geos")]
pub mod geos;
pub mod native;
#[cfg(feature = "proj")]
pub mod proj;
pub mod rstar;
