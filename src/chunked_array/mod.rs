#[allow(clippy::module_inception)]
pub mod chunked_array;

pub(crate) use chunked_array::{chunked_map, chunked_try_map};
pub use chunked_array::{
    ChunkedArray, ChunkedGeometryArray, ChunkedGeometryCollectionArray, ChunkedLineStringArray,
    ChunkedMixedGeometryArray, ChunkedMultiLineStringArray, ChunkedMultiPointArray,
    ChunkedMultiPolygonArray, ChunkedPointArray, ChunkedPolygonArray, ChunkedRectArray,
    ChunkedWKBArray,
};
