//! A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification,
//! plus algorithms implemented on and returning these GeoArrow arrays.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), deny(unused_crate_dependencies))]

pub use trait_::GeometryArrayTrait;

pub mod algorithm;
pub mod array;
pub mod chunked_array;
pub mod datatypes;
pub mod error;
pub mod geo_traits;
pub mod indexed;
pub mod io;
pub mod scalar;
pub mod schema;
pub mod table;
#[cfg(test)]
pub(crate) mod test;
pub mod trait_;
mod util;
