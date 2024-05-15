//! Implements the geometry and dataset conversion APIs defined by the [`geozero`][::geozero]
//! crate.

mod api;
pub(crate) mod array;
mod scalar;
pub(crate) mod table;

pub use api::{FromEWKB, FromWKT};
pub use array::{
    ToLineStringArray, ToMixedArray, ToMultiLineStringArray, ToMultiPointArray,
    ToMultiPolygonArray, ToPointArray, ToPolygonArray,
};
pub use scalar::ToGeometry;
pub use table::RecordBatchReader;
