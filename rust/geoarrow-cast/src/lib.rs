#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod cast;
pub mod downcast;
mod force_2d;
pub(crate) mod util;

pub use force_2d::force_2d;
