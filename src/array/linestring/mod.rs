//! Contains the [`LineStringArray`] and [`LineStringBuilder`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use builder::LineStringBuilder;
pub use capacity::LineStringCapacity;

mod array;
pub(crate) mod builder;
mod capacity;
