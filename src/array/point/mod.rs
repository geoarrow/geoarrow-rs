//! Contains the [`PointArray`] and [`PointBuilder`] for arrays of Point geometries.

pub use array::PointArray;
pub use builder::PointBuilder;
// pub use capacity::PointCapacity;

mod array;
pub(crate) mod builder;
mod capacity;
