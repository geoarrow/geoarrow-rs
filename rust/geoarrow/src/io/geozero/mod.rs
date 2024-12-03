//! Implements the geometry and dataset conversion APIs defined by the [`geozero`]
//! crate.

pub(crate) mod array;
mod scalar;
pub(crate) mod table;

pub use array::{
    ToLineStringArray, ToMixedArray, ToMultiLineStringArray, ToMultiPointArray,
    ToMultiPolygonArray, ToPointArray, ToPolygonArray,
};
pub use scalar::ToGeometry;
