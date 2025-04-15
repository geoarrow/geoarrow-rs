#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![cfg_attr(not(test), deny(unused_crate_dependencies))]

pub mod array;
pub mod builder;
pub mod capacity;
pub mod cast;
pub mod crs;
mod datatypes;
mod eq;
pub mod error;
#[cfg(feature = "geozero")]
pub mod geozero;
pub mod scalar;
mod trait_;
pub(crate) mod util;

pub use datatypes::GeoArrowType;
pub use trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};

#[cfg(test)]
pub(crate) mod test;
