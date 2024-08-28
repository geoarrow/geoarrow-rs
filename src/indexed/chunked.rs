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

pub type IndexedChunkedPointArray<const D: usize> = IndexedChunkedGeometryArray<PointArray<D>>;
pub type IndexedChunkedLineStringArray<O, const D: usize> =
    IndexedChunkedGeometryArray<LineStringArray<O, D>>;
pub type IndexedChunkedPolygonArray<O, const D: usize> =
    IndexedChunkedGeometryArray<PolygonArray<O, D>>;
pub type IndexedChunkedMultiPointArray<O, const D: usize> =
    IndexedChunkedGeometryArray<MultiPointArray<O, D>>;
pub type IndexedChunkedMultiLineStringArray<O, const D: usize> =
    IndexedChunkedGeometryArray<MultiLineStringArray<O, D>>;
pub type IndexedChunkedMultiPolygonArray<O, const D: usize> =
    IndexedChunkedGeometryArray<MultiPolygonArray<O, D>>;
pub type IndexedChunkedMixedGeometryArray<O, const D: usize> =
    IndexedChunkedGeometryArray<MixedGeometryArray<O, D>>;
pub type IndexedChunkedGeometryCollectionArray<O, const D: usize> =
    IndexedChunkedGeometryArray<GeometryCollectionArray<O, D>>;
#[allow(dead_code)]
pub type IndexedChunkedWKBArray<O> = IndexedChunkedGeometryArray<WKBArray<O>>;
#[allow(dead_code)]
pub type IndexedChunkedRectArray<const D: usize> = IndexedChunkedGeometryArray<RectArray<D>>;
#[allow(dead_code)]
pub type IndexedChunkedUnknownGeometryArray =
    IndexedChunkedGeometryArray<Arc<dyn ChunkedGeometryArrayTrait>>;
