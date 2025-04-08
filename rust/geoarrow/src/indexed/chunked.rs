use std::sync::Arc;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::NativeArray;
use crate::array::*;
use crate::chunked_array::*;
use crate::indexed::array::IndexedGeometryArray;

pub struct IndexedChunkedGeometryArray<G: NativeArray> {
    pub(crate) chunks: Vec<IndexedGeometryArray<G>>,
}

impl<G: NativeArray> IndexedChunkedGeometryArray<G> {
    #[allow(dead_code)]
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
pub type IndexedChunkedLineStringArray = IndexedChunkedGeometryArray<LineStringArray>;
pub type IndexedChunkedPolygonArray = IndexedChunkedGeometryArray<PolygonArray>;
pub type IndexedChunkedMultiPointArray = IndexedChunkedGeometryArray<MultiPointArray>;
pub type IndexedChunkedMultiLineStringArray = IndexedChunkedGeometryArray<MultiLineStringArray>;
pub type IndexedChunkedMultiPolygonArray = IndexedChunkedGeometryArray<MultiPolygonArray>;
pub type IndexedChunkedMixedGeometryArray = IndexedChunkedGeometryArray<MixedGeometryArray>;
pub type IndexedChunkedGeometryCollectionArray =
    IndexedChunkedGeometryArray<GeometryCollectionArray>;
#[allow(dead_code)]
pub type IndexedChunkedWKBArray<O> = IndexedChunkedGeometryArray<WKBArray<O>>;
#[allow(dead_code)]
pub type IndexedChunkedRectArray = IndexedChunkedGeometryArray<RectArray>;
#[allow(dead_code)]
pub type IndexedChunkedUnknownGeometryArray =
    IndexedChunkedGeometryArray<Arc<dyn ChunkedNativeArray>>;
