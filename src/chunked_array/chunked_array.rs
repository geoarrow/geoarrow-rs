use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::OffsetSizeTrait;
use arrow_array::Array;
use arrow_schema::{DataType, Field};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
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
        if !chunks
            .windows(2)
            .all(|w| w[0].data_type() == w[1].data_type())
        {
            // TODO: switch to try_new with Err
            panic!("All data types should be the same.")
        }

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

    pub fn null_count(&self) -> usize {
        self.chunks()
            .iter()
            .fold(0, |acc, chunk| acc + chunk.null_count())
    }

    pub fn chunks(&self) -> &[A] {
        self.chunks.as_slice()
    }

    #[allow(dead_code)]
    pub fn map<F: Fn(&A) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
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

    pub fn try_map<F: Fn(&A) -> Result<R> + Sync + Send, R: Send>(
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

impl<A: Array> AsRef<[A]> for ChunkedArray<A> {
    fn as_ref(&self) -> &[A] {
        &self.chunks
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

    pub fn data_type(&self) -> GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    pub fn into_map<F: Fn(G) -> R + Sync + Send, R: Send>(self, map_op: F) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .into_par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.into_iter().map(map_op).collect()
        }
    }

    pub fn map<F: Fn(&G) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
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

    pub fn try_map<F: Fn(&G) -> Result<R> + Sync + Send, R: Send>(
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

impl<'a, G: GeometryArrayTrait + GeometryArrayAccessor<'a>> ChunkedGeometryArray<G> {
    pub fn value(&'a self, index: usize) -> G::Item {
        assert!(index <= self.len());
        let mut index = index;
        for chunk in self.chunks() {
            if index >= chunk.len() {
                index -= chunk.len();
            } else {
                return chunk.value(index);
            }
        }
        unreachable!()
    }

    pub fn get(&'a self, index: usize) -> Option<G::Item> {
        assert!(index <= self.len());
        let mut index = index;
        for chunk in self.chunks() {
            if index >= chunk.len() {
                index -= chunk.len();
            } else {
                return chunk.get(index);
            }
        }
        unreachable!()
    }
}

impl<G: GeometryArrayTrait> TryFrom<Vec<G>> for ChunkedGeometryArray<G> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<G>) -> Result<Self> {
        Ok(Self::new(value))
    }
}

pub type ChunkedPointArray<const D: usize> = ChunkedGeometryArray<PointArray<D>>;
pub type ChunkedLineStringArray<O, const D: usize> = ChunkedGeometryArray<LineStringArray<O, D>>;
pub type ChunkedPolygonArray<O, const D: usize> = ChunkedGeometryArray<PolygonArray<O, D>>;
pub type ChunkedMultiPointArray<O, const D: usize> = ChunkedGeometryArray<MultiPointArray<O, D>>;
pub type ChunkedMultiLineStringArray<O, const D: usize> =
    ChunkedGeometryArray<MultiLineStringArray<O, D>>;
pub type ChunkedMultiPolygonArray<O, const D: usize> =
    ChunkedGeometryArray<MultiPolygonArray<O, D>>;
pub type ChunkedMixedGeometryArray<O, const D: usize> =
    ChunkedGeometryArray<MixedGeometryArray<O, D>>;
pub type ChunkedGeometryCollectionArray<O, const D: usize> =
    ChunkedGeometryArray<GeometryCollectionArray<O, D>>;
pub type ChunkedWKBArray<O> = ChunkedGeometryArray<WKBArray<O>>;
pub type ChunkedRectArray<const D: usize> = ChunkedGeometryArray<RectArray<D>>;
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
    fn data_type(&self) -> GeoDataType;

    /// Returns an Arrow [`Field`] describing this chunked array. This field will always have the
    /// `ARROW:extension:name` key of the field metadata set, signifying that it describes a
    /// GeoArrow extension type.
    fn extension_field(&self) -> Arc<Field>;

    /// Access the geometry chunks contained within this chunked array.
    fn geometry_chunks(&self) -> Vec<&dyn GeometryArrayTrait>;

    /// The number of chunks in this chunked array.
    fn num_chunks(&self) -> usize;

    fn as_ref(&self) -> &dyn ChunkedGeometryArrayTrait;

    fn array_refs(&self) -> Vec<Arc<dyn Array>>;
}

impl<const D: usize> ChunkedGeometryArrayTrait for ChunkedPointArray<D> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> GeoDataType {
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

    fn as_ref(&self) -> &dyn ChunkedGeometryArrayTrait {
        self
    }

    fn array_refs(&self) -> Vec<Arc<dyn Array>> {
        self.chunks
            .iter()
            .map(|chunk| chunk.to_array_ref())
            .collect()
    }
}

impl<O: OffsetSizeTrait> ChunkedGeometryArrayTrait for ChunkedWKBArray<O> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> GeoDataType {
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

    fn as_ref(&self) -> &dyn ChunkedGeometryArrayTrait {
        self
    }

    fn array_refs(&self) -> Vec<Arc<dyn Array>> {
        self.chunks
            .iter()
            .map(|chunk| chunk.to_array_ref())
            .collect()
    }
}

macro_rules! impl_trait {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait, const D: usize> ChunkedGeometryArrayTrait for $chunked_array {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn data_type(&self) -> GeoDataType {
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

            fn as_ref(&self) -> &dyn ChunkedGeometryArrayTrait {
                self
            }

            fn array_refs(&self) -> Vec<Arc<dyn Array>> {
                self.chunks
                    .iter()
                    .map(|chunk| chunk.to_array_ref())
                    .collect()
            }
        }
    };
}

impl_trait!(ChunkedLineStringArray<O, D>);
impl_trait!(ChunkedPolygonArray<O, D>);
impl_trait!(ChunkedMultiPointArray<O, D>);
impl_trait!(ChunkedMultiLineStringArray<O, D>);
impl_trait!(ChunkedMultiPolygonArray<O, D>);
impl_trait!(ChunkedMixedGeometryArray<O, D>);
impl_trait!(ChunkedGeometryCollectionArray<O, D>);

impl<const D: usize> ChunkedGeometryArrayTrait for ChunkedRectArray<D> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> GeoDataType {
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

    fn as_ref(&self) -> &dyn ChunkedGeometryArrayTrait {
        self
    }

    fn array_refs(&self) -> Vec<Arc<dyn Array>> {
        self.chunks
            .iter()
            .map(|chunk| chunk.to_array_ref())
            .collect()
    }
}

/// Construct
/// Does **not** parse WKB. Will return a ChunkedWKBArray for WKB input.
pub fn from_arrow_chunks(
    chunks: &[&dyn Array],
    field: &Field,
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    if chunks.is_empty() {
        return Err(GeoArrowError::General(
            "Cannot create zero-length chunked array".to_string(),
        ));
    }

    macro_rules! impl_downcast {
        ($array:ty) => {
            Ok(Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| <$array>::try_from((*array, field)))
                    .collect::<Result<Vec<_>>>()?,
            )))
        };
    }
    use GeoDataType::*;

    let geo_data_type = GeoDataType::try_from(field)?;
    match geo_data_type {
        Point(_, Dimension::XY) => impl_downcast!(PointArray<2>),
        LineString(_, Dimension::XY) => impl_downcast!(LineStringArray<i32, 2>),
        LargeLineString(_, Dimension::XY) => impl_downcast!(LineStringArray<i64, 2>),
        Polygon(_, Dimension::XY) => impl_downcast!(PolygonArray<i32, 2>),
        LargePolygon(_, Dimension::XY) => impl_downcast!(PolygonArray<i64, 2>),
        MultiPoint(_, Dimension::XY) => impl_downcast!(MultiPointArray<i32, 2>),
        LargeMultiPoint(_, Dimension::XY) => impl_downcast!(MultiPointArray<i64, 2>),
        MultiLineString(_, Dimension::XY) => impl_downcast!(MultiLineStringArray<i32, 2>),
        LargeMultiLineString(_, Dimension::XY) => impl_downcast!(MultiLineStringArray<i64, 2>),
        MultiPolygon(_, Dimension::XY) => impl_downcast!(MultiPolygonArray<i32, 2>),
        LargeMultiPolygon(_, Dimension::XY) => impl_downcast!(MultiPolygonArray<i64, 2>),
        Mixed(_, Dimension::XY) => impl_downcast!(MixedGeometryArray<i32, 2>),
        LargeMixed(_, Dimension::XY) => impl_downcast!(MixedGeometryArray<i64, 2>),
        GeometryCollection(_, Dimension::XY) => impl_downcast!(GeometryCollectionArray<i32, 2>),
        LargeGeometryCollection(_, Dimension::XY) => {
            impl_downcast!(GeometryCollectionArray<i64, 2>)
        }
        Rect(Dimension::XY) => impl_downcast!(RectArray<2>),

        Point(_, Dimension::XYZ) => impl_downcast!(PointArray<3>),
        LineString(_, Dimension::XYZ) => impl_downcast!(LineStringArray<i32, 3>),
        LargeLineString(_, Dimension::XYZ) => impl_downcast!(LineStringArray<i64, 3>),
        Polygon(_, Dimension::XYZ) => impl_downcast!(PolygonArray<i32, 3>),
        LargePolygon(_, Dimension::XYZ) => impl_downcast!(PolygonArray<i64, 3>),
        MultiPoint(_, Dimension::XYZ) => impl_downcast!(MultiPointArray<i32, 3>),
        LargeMultiPoint(_, Dimension::XYZ) => impl_downcast!(MultiPointArray<i64, 3>),
        MultiLineString(_, Dimension::XYZ) => impl_downcast!(MultiLineStringArray<i32, 3>),
        LargeMultiLineString(_, Dimension::XYZ) => impl_downcast!(MultiLineStringArray<i64, 3>),
        MultiPolygon(_, Dimension::XYZ) => impl_downcast!(MultiPolygonArray<i32, 3>),
        LargeMultiPolygon(_, Dimension::XYZ) => impl_downcast!(MultiPolygonArray<i64, 3>),
        Mixed(_, Dimension::XYZ) => impl_downcast!(MixedGeometryArray<i32, 3>),
        LargeMixed(_, Dimension::XYZ) => impl_downcast!(MixedGeometryArray<i64, 3>),
        GeometryCollection(_, Dimension::XYZ) => impl_downcast!(GeometryCollectionArray<i32, 3>),
        LargeGeometryCollection(_, Dimension::XYZ) => {
            impl_downcast!(GeometryCollectionArray<i64, 3>)
        }
        Rect(Dimension::XYZ) => impl_downcast!(RectArray<3>),

        WKB => impl_downcast!(WKBArray<i32>),
        LargeWKB => impl_downcast!(WKBArray<i64>),
    }
}

pub fn from_geoarrow_chunks(
    chunks: &[&dyn GeometryArrayTrait],
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    if chunks.is_empty() {
        return Err(GeoArrowError::General(
            "Cannot create zero-length chunked array".to_string(),
        ));
    }

    let mut data_types = HashSet::new();
    chunks.iter().for_each(|chunk| {
        data_types.insert(chunk.as_ref().data_type());
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
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match data_types.drain().next().unwrap() {
            Point(_, Dimension::XY) => impl_downcast!(as_point_2d),
            LineString(_, Dimension::XY) => impl_downcast!(as_line_string_2d),
            LargeLineString(_, Dimension::XY) => impl_downcast!(as_large_line_string_2d),
            Polygon(_, Dimension::XY) => impl_downcast!(as_polygon_2d),
            LargePolygon(_, Dimension::XY) => impl_downcast!(as_large_polygon_2d),
            MultiPoint(_, Dimension::XY) => impl_downcast!(as_multi_point_2d),
            LargeMultiPoint(_, Dimension::XY) => impl_downcast!(as_large_multi_point_2d),
            MultiLineString(_, Dimension::XY) => impl_downcast!(as_multi_line_string_2d),
            LargeMultiLineString(_, Dimension::XY) => impl_downcast!(as_large_multi_line_string_2d),
            MultiPolygon(_, Dimension::XY) => impl_downcast!(as_multi_polygon_2d),
            LargeMultiPolygon(_, Dimension::XY) => impl_downcast!(as_large_multi_polygon_2d),
            Mixed(_, Dimension::XY) => impl_downcast!(as_mixed_2d),
            LargeMixed(_, Dimension::XY) => impl_downcast!(as_large_mixed_2d),
            GeometryCollection(_, Dimension::XY) => impl_downcast!(as_geometry_collection_2d),
            LargeGeometryCollection(_, Dimension::XY) => {
                impl_downcast!(as_large_geometry_collection_2d)
            }
            WKB => impl_downcast!(as_wkb),
            LargeWKB => impl_downcast!(as_large_wkb),
            Rect(Dimension::XY) => impl_downcast!(as_rect_2d),
            _ => todo!("3d downcasting"),
        };
        Ok(result)
    } else {
        Err(GeoArrowError::General(format!(
            "Handling multiple geometry types in `from_geoarrow_chunks` not yet implemented. Received {:?}", data_types
        )))
    }
}
