use arrow_array::Array;

use crate::array::*;
use crate::GeometryArrayTrait;

pub struct ChunkedArray<A: Array> {
    pub(crate) chunks: Vec<A>,
    length: usize,
}

impl<A: Array> ChunkedArray<A> {
    pub fn new(chunks: Vec<A>) -> Self {
        let mut length = 0;
        chunks.iter().for_each(|x| length += x.len());
        // TODO: assert all equal data types
        // chunks.iter().map(|x| x.data_type())
        Self { chunks, length }
    }

    pub fn into_inner(self) -> Vec<A> {
        self.chunks
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct ChunkedGeometryArray<G: GeometryArrayTrait> {
    pub(crate) chunks: Vec<G>,
    length: usize,
}

impl<G: GeometryArrayTrait> ChunkedGeometryArray<G> {
    pub fn new(chunks: Vec<G>) -> Self {
        // TODO: assert all equal extension fields
        let mut length = 0;
        chunks.iter().for_each(|x| length += x.len());
        Self { chunks, length }
    }

    pub fn into_inner(self) -> Vec<G> {
        self.chunks
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type ChunkedPointArray = ChunkedGeometryArray<PointArray>;
pub type ChunkedLineStringArray<O> = ChunkedGeometryArray<LineStringArray<O>>;
pub type ChunkedPolygonArray<O> = ChunkedGeometryArray<PolygonArray<O>>;
pub type ChunkedMultiPointArray<O> = ChunkedGeometryArray<MultiPointArray<O>>;
pub type ChunkedMultiLineStringArray<O> = ChunkedGeometryArray<MultiLineStringArray<O>>;
pub type ChunkedMultiPolygonArray<O> = ChunkedGeometryArray<MultiPolygonArray<O>>;
pub type ChunkedMixedGeometryArray<O> = ChunkedGeometryArray<MixedGeometryArray<O>>;
pub type ChunkedGeometryCollectionArray<O> = ChunkedGeometryArray<GeometryCollectionArray<O>>;
pub type ChunkedWKBArray<O> = ChunkedGeometryArray<WKBArray<O>>;
