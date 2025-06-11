#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![cfg_attr(not(test), deny(unused_crate_dependencies))]

pub mod array;
pub mod builder;
pub mod capacity;
pub mod cast;
mod eq;
#[cfg(feature = "geozero")]
pub mod geozero;
pub mod scalar;
mod trait_;
pub(crate) mod util;

pub use trait_::{
    GeoArrowArray, GeoArrowArrayAccessor, GeoArrowArrayIterator, GeoArrowArrayReader, IntoArrow,
};

#[cfg(any(test, feature = "test-data"))]
#[allow(missing_docs)]
pub mod test;
