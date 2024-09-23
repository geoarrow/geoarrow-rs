use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Field;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};

/// A wrapper around a ChunkedNativeArray of unknown type
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ChunkedNativeArrayDyn(Arc<dyn ChunkedNativeArray>);

impl ChunkedNativeArrayDyn {
    pub fn new(array: Arc<dyn ChunkedNativeArray>) -> Self {
        Self(array)
    }

    /// Constructs a chunked geometry array from arrow chunks.
    ///
    /// Does **not** parse WKB. Will return a ChunkedWKBArray for WKB input.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{ArrayBase, NativeArray, array::PointArray};
    /// use geoarrow::chunked_array::ChunkedNativeArrayDyn;
    /// use std::sync::Arc;
    ///
    /// let array: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let field = array.extension_field();
    /// let array = array.into_array_ref();
    /// let chunks = vec![array.as_ref()];
    /// let chunked_array = ChunkedNativeArrayDyn::from_arrow_chunks(chunks.as_slice(), &field).unwrap();
    /// ```
    pub fn from_arrow_chunks(chunks: &[&dyn Array], field: &Field) -> Result<Self> {
        if chunks.is_empty() {
            return Err(GeoArrowError::General(
                "Cannot create zero-length chunked array".to_string(),
            ));
        }

        macro_rules! impl_downcast {
            ($array:ty) => {
                Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| <$array>::try_from((*array, field)))
                        .collect::<Result<Vec<_>>>()?,
                ))
            };
        }
        use NativeType::*;

        let typ = NativeType::try_from(field)?;
        let ca: Arc<dyn ChunkedNativeArray> = match typ {
            Point(_, Dimension::XY) => impl_downcast!(PointArray<2>),
            LineString(_, Dimension::XY) => impl_downcast!(LineStringArray<i32, 2>),
            LargeLineString(_, Dimension::XY) => impl_downcast!(LineStringArray<i64, 2>),
            Polygon(_, Dimension::XY) => impl_downcast!(PolygonArray<i32, 2>),
            LargePolygon(_, Dimension::XY) => impl_downcast!(PolygonArray<i64, 2>),
            MultiPoint(_, Dimension::XY) => impl_downcast!(MultiPointArray<i32, 2>),
            LargeMultiPoint(_, Dimension::XY) => impl_downcast!(MultiPointArray<i64, 2>),
            MultiLineString(_, Dimension::XY) => impl_downcast!(MultiLineStringArray<i32, 2>),
            LargeMultiLineString(_, Dimension::XY) => {
                impl_downcast!(MultiLineStringArray<i64, 2>)
            }
            MultiPolygon(_, Dimension::XY) => impl_downcast!(MultiPolygonArray<i32, 2>),
            LargeMultiPolygon(_, Dimension::XY) => impl_downcast!(MultiPolygonArray<i64, 2>),
            Mixed(_, Dimension::XY) => impl_downcast!(MixedGeometryArray<i32, 2>),
            LargeMixed(_, Dimension::XY) => impl_downcast!(MixedGeometryArray<i64, 2>),
            GeometryCollection(_, Dimension::XY) => {
                impl_downcast!(GeometryCollectionArray<i32, 2>)
            }
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
            LargeMultiLineString(_, Dimension::XYZ) => {
                impl_downcast!(MultiLineStringArray<i64, 3>)
            }
            MultiPolygon(_, Dimension::XYZ) => impl_downcast!(MultiPolygonArray<i32, 3>),
            LargeMultiPolygon(_, Dimension::XYZ) => impl_downcast!(MultiPolygonArray<i64, 3>),
            Mixed(_, Dimension::XYZ) => impl_downcast!(MixedGeometryArray<i32, 3>),
            LargeMixed(_, Dimension::XYZ) => impl_downcast!(MixedGeometryArray<i64, 3>),
            GeometryCollection(_, Dimension::XYZ) => {
                impl_downcast!(GeometryCollectionArray<i32, 3>)
            }
            LargeGeometryCollection(_, Dimension::XYZ) => {
                impl_downcast!(GeometryCollectionArray<i64, 3>)
            }
            Rect(Dimension::XYZ) => impl_downcast!(RectArray<3>),
        };
        Ok(Self(ca))
    }

    /// Creates a chunked geometry array from geoarrow chunks.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{NativeArray, array::PointArray};
    /// use geoarrow::chunked_array::ChunkedNativeArrayDyn;
    ///
    /// let array_0: PointArray<2> = vec![&geo::point!(x: 1., y: 2.)].as_slice().into();
    /// let array_1: PointArray<2> = vec![&geo::point!(x: 3., y: 4.)].as_slice().into();
    /// let chunks = vec![array_0.as_ref(), array_1.as_ref()];
    /// let chunked_array = ChunkedNativeArrayDyn::from_geoarrow_chunks(chunks.as_slice()).unwrap();
    /// ```
    pub fn from_geoarrow_chunks(chunks: &[&dyn NativeArray]) -> Result<Self> {
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
                ($cast_func:ident, $dim:expr) => {
                    Arc::new(ChunkedGeometryArray::new(
                        chunks
                            .iter()
                            .map(|chunk| chunk.as_ref().$cast_func::<$dim>().clone())
                            .collect(),
                    ))
                };
            }

            use NativeType::*;
            let result: Arc<dyn ChunkedNativeArray> = match data_types.drain().next().unwrap() {
                Point(_, Dimension::XY) => impl_downcast!(as_point, 2),
                LineString(_, Dimension::XY) => impl_downcast!(as_line_string, 2),
                LargeLineString(_, Dimension::XY) => impl_downcast!(as_large_line_string, 2),
                Polygon(_, Dimension::XY) => impl_downcast!(as_polygon, 2),
                LargePolygon(_, Dimension::XY) => impl_downcast!(as_large_polygon, 2),
                MultiPoint(_, Dimension::XY) => impl_downcast!(as_multi_point, 2),
                LargeMultiPoint(_, Dimension::XY) => impl_downcast!(as_large_multi_point, 2),
                MultiLineString(_, Dimension::XY) => impl_downcast!(as_multi_line_string, 2),
                LargeMultiLineString(_, Dimension::XY) => {
                    impl_downcast!(as_large_multi_line_string, 2)
                }
                MultiPolygon(_, Dimension::XY) => impl_downcast!(as_multi_polygon, 2),
                LargeMultiPolygon(_, Dimension::XY) => impl_downcast!(as_large_multi_polygon, 2),
                Mixed(_, Dimension::XY) => impl_downcast!(as_mixed, 2),
                LargeMixed(_, Dimension::XY) => impl_downcast!(as_large_mixed, 2),
                GeometryCollection(_, Dimension::XY) => impl_downcast!(as_geometry_collection, 2),
                LargeGeometryCollection(_, Dimension::XY) => {
                    impl_downcast!(as_large_geometry_collection, 2)
                }
                Point(_, Dimension::XYZ) => impl_downcast!(as_point, 3),
                LineString(_, Dimension::XYZ) => impl_downcast!(as_line_string, 3),
                LargeLineString(_, Dimension::XYZ) => impl_downcast!(as_large_line_string, 3),
                Polygon(_, Dimension::XYZ) => impl_downcast!(as_polygon, 3),
                LargePolygon(_, Dimension::XYZ) => impl_downcast!(as_large_polygon, 3),
                MultiPoint(_, Dimension::XYZ) => impl_downcast!(as_multi_point, 3),
                LargeMultiPoint(_, Dimension::XYZ) => impl_downcast!(as_large_multi_point, 3),
                MultiLineString(_, Dimension::XYZ) => impl_downcast!(as_multi_line_string, 3),
                LargeMultiLineString(_, Dimension::XYZ) => {
                    impl_downcast!(as_large_multi_line_string, 3)
                }
                MultiPolygon(_, Dimension::XYZ) => impl_downcast!(as_multi_polygon, 3),
                LargeMultiPolygon(_, Dimension::XYZ) => impl_downcast!(as_large_multi_polygon, 3),
                Mixed(_, Dimension::XYZ) => impl_downcast!(as_mixed, 3),
                LargeMixed(_, Dimension::XYZ) => impl_downcast!(as_large_mixed, 3),
                GeometryCollection(_, Dimension::XYZ) => impl_downcast!(as_geometry_collection, 3),
                LargeGeometryCollection(_, Dimension::XYZ) => {
                    impl_downcast!(as_large_geometry_collection, 3)
                }
                Rect(Dimension::XY) => impl_downcast!(as_rect, 2),
                Rect(Dimension::XYZ) => impl_downcast!(as_rect, 3),
            };
            Ok(Self(result))
        } else {
            Err(GeoArrowError::General(format!(
            "Handling multiple geometry types in `from_geoarrow_chunks` not yet implemented. Received {:?}", data_types
        )))
        }
    }

    pub fn inner(&self) -> &Arc<dyn ChunkedNativeArray> {
        &self.0
    }

    pub fn into_inner(self) -> Arc<dyn ChunkedNativeArray> {
        self.0
    }
}
