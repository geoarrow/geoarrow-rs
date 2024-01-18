use std::any::Any;
use std::collections::HashSet;
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

/// A collection of Arrow arrays of the same type.
///
/// This can be thought of as a column in a table, as Table objects normally have internal batches.
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

/// A collection of GeoArrow geometry arrays of the same type.
///
/// This can be thought of as a geometry column in a table, as Table objects normally have internal
/// batches.
///
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
#[allow(dead_code)]
pub type ChunkedUnknownGeometryArray = ChunkedGeometryArray<Arc<dyn GeometryArrayTrait>>;

/// A trait implemented by all chunked geometry arrays.
///
/// This trait is often used for downcasting. For example, the [`from_geoarrow_chunks`] function
/// returns a dynamically-typed `Arc<dyn ChunkedGeometryArrayTrait>`. To downcast into a
/// strongly-typed chunked array, use `as_any` with the `data_type` method to discern which chunked
/// array type to pass to `downcast_ref`.
pub trait ChunkedGeometryArrayTrait: std::fmt::Debug + Send + Sync {
    /// Returns the array as [`Any`] so that it can be
    /// downcasted to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns a reference to the [`GeoDataType`] of this array.
    fn data_type(&self) -> &GeoDataType;

    /// Returns an Arrow [`Field`] describing this chunked array. This field will always have the
    /// `ARROW:extension:name` key of the field metadata set, signifying that it describes a
    /// GeoArrow extension type.
    fn extension_field(&self) -> Arc<Field>;

    /// Access the geometry chunks contained within this chunked array.
    fn geometry_chunks(&self) -> Vec<&dyn GeometryArrayTrait>;

    /// The number of chunks in this chunked array.
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

    fn geometry_chunks(&self) -> Vec<&dyn GeometryArrayTrait> {
        self.chunks.iter().map(|chunk| chunk.as_ref()).collect()
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

            fn geometry_chunks(&self) -> Vec<&dyn GeometryArrayTrait> {
                self.chunks.iter().map(|chunk| chunk.as_ref()).collect()
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

    fn geometry_chunks(&self) -> Vec<&dyn GeometryArrayTrait> {
        self.chunks.iter().map(|chunk| chunk.as_ref()).collect()
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }
}

/// Construct
/// Does **not** parse WKB. Will return a ChunkedWKBArray for WKB input.
pub fn from_arrow_chunks<A: AsRef<dyn Array>>(
    chunks: &[A],
    field: &Field,
    parse_wkb: bool,
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    macro_rules! impl_downcast {
        ($array:ty) => {
            Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| <$array>::try_from(array.as_ref()))
                    .collect::<Result<Vec<_>>>()?,
            ))
        };
    }
    use GeoDataType::*;

    let geo_data_type = GeoDataType::try_from(field)?;
    match geo_data_type {
        Point(_) => impl_downcast!(PointArray),
        LineString(_) => impl_downcast!(LineStringArray<i32>),
        LargeLineString(_) => impl_downcast!(LineStringArray<i64>),
        Polygon(_) => impl_downcast!(PolygonArray<i32>),
        LargePolygon(_) => impl_downcast!(PolygonArray<i64>),
        MultiPoint(_) => impl_downcast!(MultiPointArray<i32>),
        LargeMultiPoint(_) => impl_downcast!(MultiPointArray<i64>),
        MultiLineString(_) => impl_downcast!(MultiLineStringArray<i32>),
        LargeMultiLineString(_) => impl_downcast!(MultiLineStringArray<i64>),
        MultiPolygon(_) => impl_downcast!(MultiPolygonArray<i32>),
        LargeMultiPolygon(_) => impl_downcast!(MultiPolygonArray<i64>),
        Mixed(_) => impl_downcast!(MixedGeometryArray<i32>),
        LargeMixed(_) => impl_downcast!(MixedGeometryArray<i64>),
        GeometryCollection(_) => impl_downcast!(GeometryCollectionArray<i32>),
        LargeGeometryCollection(_) => impl_downcast!(GeometryCollectionArray<i64>),
        WKB => impl_downcast!(WKBArray<i32>),
        LargeWKB => impl_downcast!(WKBArray<i64>),
        Rect => impl_downcast!(RectArray),
        _ => todo!(),
    }
}

pub fn from_geoarrow_chunks<G: AsRef<dyn GeometryArrayTrait>>(
    chunks: &[G],
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    let mut data_types = HashSet::new();
    chunks.iter().for_each(|chunk| {
        data_types.insert(chunk.data_type());
    });

    if data_types.len() == 1 {
        macro_rules! impl_downcast {
            ($cast_func:ident) => {
                Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_ref().$cast_func().clone())
                        .collect(),
                ))
            };
        }

        use GeoDataType::*;
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match *data_types.drain().next().unwrap() {
            Point(_) => impl_downcast!(as_point),
            LineString(_) => impl_downcast!(as_line_string),
            LargeLineString(_) => impl_downcast!(as_large_line_string),
            Polygon(_) => impl_downcast!(as_polygon),
            LargePolygon(_) => impl_downcast!(as_large_polygon),
            MultiPoint(_) => impl_downcast!(as_multi_point),
            LargeMultiPoint(_) => impl_downcast!(as_large_multi_point),
            MultiLineString(_) => impl_downcast!(as_multi_line_string),
            LargeMultiLineString(_) => impl_downcast!(as_large_multi_line_string),
            MultiPolygon(_) => impl_downcast!(as_polygon),
            LargeMultiPolygon(_) => impl_downcast!(as_large_polygon),
            Mixed(_) => impl_downcast!(as_mixed),
            LargeMixed(_) => impl_downcast!(as_large_mixed),
            GeometryCollection(_) => impl_downcast!(as_geometry_collection),
            LargeGeometryCollection(_) => impl_downcast!(as_large_geometry_collection),
            WKB => impl_downcast!(as_wkb),
            LargeWKB => impl_downcast!(as_large_wkb),
            Rect => impl_downcast!(as_rect),
        };
        Ok(result)
    } else {
        todo!()
    }
}
