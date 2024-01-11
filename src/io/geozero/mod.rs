//! Implements the geometry and dataset conversion APIs defined by the [`geozero`][::geozero]
//! crate.

mod api;
pub(crate) mod array;
mod scalar;
pub(crate) mod table;

pub use api::{FromEWKB, FromWKT};
pub use array::ToLineStringArray;
pub use array::ToMixedArray;
pub use array::ToMultiLineStringArray;
pub use array::ToMultiPointArray;
pub use array::ToMultiPolygonArray;
pub use array::ToPointArray;
pub use array::ToPolygonArray;
