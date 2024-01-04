use std::any::Any;
use std::sync::Arc;

use arrow::array::OffsetSizeTrait;
use arrow_array::Array;
use arrow_schema::{DataType, Field};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;

#[derive(Debug, Clone, PartialEq)]
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

    pub fn data_type(&self) -> &DataType {
        self.chunks.first().unwrap().data_type()
    }

    pub fn chunks(&self) -> &[A] {
        self.chunks.as_slice()
    }

    #[allow(dead_code)]
    pub(crate) fn map<F: Fn(&A) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
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

    pub(crate) fn try_map<F: Fn(&A) -> Result<R> + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Result<Vec<R>> {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

impl<A: Array> TryFrom<Vec<A>> for ChunkedArray<A> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<A>) -> Result<Self> {
        Ok(Self::new(value))
    }
}

/// ## Invariants:
///
/// - Must have at least one chunk
#[derive(Debug, Clone, PartialEq)]
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

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    pub fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
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

    pub fn chunks(&self) -> &[G] {
        self.chunks.as_slice()
    }

    pub fn data_type(&self) -> &GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    pub(crate) fn map<F: Fn(&G) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
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

    pub(crate) fn try_map<F: Fn(&G) -> Result<R> + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Result<Vec<R>> {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

impl<G: GeometryArrayTrait> TryFrom<Vec<G>> for ChunkedGeometryArray<G> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<G>) -> Result<Self> {
        Ok(Self::new(value))
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
pub type ChunkedRectArray = ChunkedGeometryArray<RectArray>;
pub type ChunkedUnknownGeometryArray = ChunkedGeometryArray<Arc<dyn GeometryArrayTrait>>;

pub trait ChunkedGeometryArrayTrait: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn data_type(&self) -> &GeoDataType;

    fn extension_field(&self) -> Arc<Field>;

    fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>>;

    fn num_chunks(&self) -> usize;
}

impl ChunkedGeometryArrayTrait for ChunkedPointArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>> {
        self.chunks
            .iter()
            .map(|chunk| Arc::new(chunk.clone()) as Arc<dyn GeometryArrayTrait>)
            .collect()
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }
}

macro_rules! impl_trait {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait> ChunkedGeometryArrayTrait for $chunked_array {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn data_type(&self) -> &GeoDataType {
                self.chunks.first().unwrap().data_type()
            }

            // TODO: check/assert on creation that all are the same so we can be comfortable here only
            // taking the first.
            fn extension_field(&self) -> Arc<Field> {
                self.chunks.first().unwrap().extension_field()
            }

            fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>> {
                self.chunks
                    .iter()
                    .map(|chunk| Arc::new(chunk.clone()) as Arc<dyn GeometryArrayTrait>)
                    .collect()
            }

            fn num_chunks(&self) -> usize {
                self.chunks.len()
            }
        }
    };
}

impl_trait!(ChunkedLineStringArray<O>);
impl_trait!(ChunkedPolygonArray<O>);
impl_trait!(ChunkedMultiPointArray<O>);
impl_trait!(ChunkedMultiLineStringArray<O>);
impl_trait!(ChunkedMultiPolygonArray<O>);
impl_trait!(ChunkedMixedGeometryArray<O>);
impl_trait!(ChunkedGeometryCollectionArray<O>);
impl_trait!(ChunkedWKBArray<O>);

impl ChunkedGeometryArrayTrait for ChunkedRectArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>> {
        self.chunks
            .iter()
            .map(|chunk| Arc::new(chunk.clone()) as Arc<dyn GeometryArrayTrait>)
            .collect()
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }
}
