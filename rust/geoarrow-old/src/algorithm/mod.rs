//! Vectorized algorithms implemented on and returning GeoArrow arrays.

#![allow(missing_docs)] // FIXME

pub mod broadcasting;
pub mod geo;
pub mod geo_index;
#[cfg(feature = "geos")]
pub mod geos;
pub mod native;
#[cfg(feature = "polylabel")]
pub mod polylabel;
#[cfg(feature = "proj")]
pub mod proj;
pub mod rstar;
