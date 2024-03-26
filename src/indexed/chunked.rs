use std::sync::Arc;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::array::*;
use crate::chunked_array::*;
use crate::indexed::array::IndexedGeometryArray;
use crate::GeometryArrayTrait;

pub struct IndexedChunkedGeometryArray<G: GeometryArrayTrait> {
    pub(crate) chunks: Vec<IndexedGeometryArray<G>>,
}

impl<G: GeometryArrayTrait> IndexedChunkedGeometryArray<G> {
    pub fn new(chunks: Vec<G>) -> Self {
        assert!(chunks.iter().all(|chunk| chunk.null_count() == 0));
        let chunks =
            ChunkedGeometryArray::new(chunks).into_map(|chunk| IndexedGeometryArray::new(chunk));
        Self { chunks }
    }

    pub fn map<F: Fn(&IndexedGeometryArray<G>) -> R + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

pub type IndexedChunkedPointArray = IndexedChunkedGeometryArray<PointArray>;
pub type IndexedChunkedLineStringArray<O> = IndexedChunkedGeometryArray<LineStringArray<O>>;
pub type IndexedChunkedPolygonArray<O> = IndexedChunkedGeometryArray<PolygonArray<O>>;
pub type IndexedChunkedMultiPointArray<O> = IndexedChunkedGeometryArray<MultiPointArray<O>>;
pub type IndexedChunkedMultiLineStringArray<O> =
    IndexedChunkedGeometryArray<MultiLineStringArray<O>>;
pub type IndexedChunkedMultiPolygonArray<O> = IndexedChunkedGeometryArray<MultiPolygonArray<O>>;
pub type IndexedChunkedMixedGeometryArray<O> = IndexedChunkedGeometryArray<MixedGeometryArray<O>>;
pub type IndexedChunkedGeometryCollectionArray<O> =
    IndexedChunkedGeometryArray<GeometryCollectionArray<O>>;
pub type IndexedChunkedWKBArray<O> = IndexedChunkedGeometryArray<WKBArray<O>>;
pub type IndexedChunkedRectArray = IndexedChunkedGeometryArray<RectArray>;
pub type IndexedChunkedUnknownGeometryArray =
    IndexedChunkedGeometryArray<Arc<dyn ChunkedGeometryArrayTrait>>;
