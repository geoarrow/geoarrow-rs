//! Contains implementations of _chunked_ GeoArrow arrays.
//!
//! In contrast to the structures in [array](../array), these data structures only have contiguous
//! memory within each individual _chunk_. These chunked arrays are essentially wrappers around a
//! `Vec` of geometry arrays.
//!
//! Additionally, if the `rayon` feature is active, operations on chunked arrays will automatically
//! be parallelized across each chunk.

#[allow(clippy::module_inception)]
mod chunked_array;

pub use chunked_array::{
    from_arrow_chunks, from_geoarrow_chunks, ChunkedArray, ChunkedGeometryArray,
    ChunkedGeometryArrayTrait, ChunkedGeometryCollectionArray, ChunkedLineStringArray,
    ChunkedMixedGeometryArray, ChunkedMultiLineStringArray, ChunkedMultiPointArray,
    ChunkedMultiPolygonArray, ChunkedPointArray, ChunkedPolygonArray, ChunkedRectArray,
    ChunkedWKBArray,
};
