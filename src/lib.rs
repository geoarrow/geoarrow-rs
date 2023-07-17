//! A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification,
//! plus algorithms implemented on and returning these GeoArrow arrays.

pub use trait_::GeometryArrayTrait;

pub mod algorithm;
pub mod array;
pub mod error;
pub(crate) mod geo_traits;
pub(crate) mod io;
pub mod scalar;
#[cfg(test)]
pub(crate) mod test;
pub mod trait_;
mod util;
