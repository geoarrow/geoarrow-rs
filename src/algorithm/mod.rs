//! Vectorized algorithms implemented on and returning GeoArrow arrays.

pub mod broadcasting;
pub mod geo;
pub mod geo_index;
#[cfg(feature = "geodesy")]
pub mod geodesy;
#[cfg(feature = "geos")]
pub mod geos;
pub mod native;
#[cfg(feature = "polylabel")]
pub mod polylabel;
#[cfg(feature = "proj")]
pub mod proj;
pub mod rstar;
