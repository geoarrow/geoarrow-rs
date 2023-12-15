//! A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification,
//! plus algorithms implemented on and returning these GeoArrow arrays.

pub use trait_::GeometryArrayTrait;

pub mod algorithm;
pub mod array;
pub mod datatypes;
pub mod error;
pub mod geo_traits;
pub mod io;
pub mod scalar;
pub mod table;
#[cfg(test)]
pub(crate) mod test;
pub mod trait_;
mod util;

#[cfg(feature = "gdal")]
pub use gdal;
#[cfg(feature = "geos")]
pub use geos;
#[cfg(feature = "geozero")]
pub use geozero;
#[cfg(feature = "proj")]
pub use proj;
