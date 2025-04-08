#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub mod array;
pub mod builder;
pub mod capacity;
pub mod cast;
mod datatypes;
mod eq;
pub mod error;
pub mod scalar;
mod trait_;
pub(crate) mod util;

pub use datatypes::GeoArrowType;
pub use trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};

#[cfg(test)]
pub(crate) mod test;
