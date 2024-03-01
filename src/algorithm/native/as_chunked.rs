use std::sync::Arc;

use crate::chunked_array::{from_arrow_chunks, ChunkedGeometryArrayTrait};
use crate::error::Result;
use crate::GeometryArrayTrait;

// TODO: don't go through Arc<dyn Array>
// Update geometry array trait to put slice on the main trait
// Put slice() on each individual array directly, and delegate to it from geom trait
pub fn as_chunked_geometry_array(
    array: &dyn GeometryArrayTrait,
    chunk_lengths: &[usize],
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    assert_eq!(array.len(), chunk_lengths.iter().sum::<usize>());

    let mut new_chunks = Vec::with_capacity(chunk_lengths.len());
    let mut offset = 0;
    for length in chunk_lengths {
        new_chunks.push(array.to_array_ref());
        offset += length;
    }

    let array_refs = new_chunks
        .iter()
        .map(|arr| arr.as_ref())
        .collect::<Vec<_>>();
    from_arrow_chunks(array_refs.as_slice(), array.extension_field().as_ref())
}
