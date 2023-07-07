//! `geoarrow`: A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow)
//! specification.

pub use trait_::GeometryArrayTrait;

pub mod alg;
pub mod array;
pub mod error;
pub mod geo_traits;
pub mod scalar;
#[cfg(test)]
pub(crate) mod test;
pub mod trait_;
mod util;
