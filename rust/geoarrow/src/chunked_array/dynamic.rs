use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Field;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
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
    /// use geoarrow::datatypes::Dimension;
    /// use std::sync::Arc;
    ///
    /// let array: PointArray = (vec![&geo::point!(x: 1., y: 2.)].as_slice(), Dimension::XY).into();
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
            Point(_) => impl_downcast!(PointArray),
            LineString(_) => impl_downcast!(LineStringArray),
            Polygon(_) => impl_downcast!(PolygonArray),
            MultiPoint(_) => impl_downcast!(MultiPointArray),
            MultiLineString(_) => impl_downcast!(MultiLineStringArray),
            MultiPolygon(_) => impl_downcast!(MultiPolygonArray),
            GeometryCollection(_) => {
                impl_downcast!(GeometryCollectionArray)
            }
            Rect(_) => impl_downcast!(RectArray),
            Geometry(_) => impl_downcast!(GeometryArray),
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
    /// use geoarrow::datatypes::Dimension;
    ///
    /// let array_0: PointArray = (vec![&geo::point!(x: 1., y: 2.)].as_slice(), Dimension::XY).into();
    /// let array_1: PointArray = (vec![&geo::point!(x: 3., y: 4.)].as_slice(), Dimension::XY).into();
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
            }

            use NativeType::*;
            let result: Arc<dyn ChunkedNativeArray> = match data_types.drain().next().unwrap() {
                Point(_) => impl_downcast!(as_point),
                LineString(_) => impl_downcast!(as_line_string),
                Polygon(_) => impl_downcast!(as_polygon),
                MultiPoint(_) => impl_downcast!(as_multi_point),
                MultiLineString(_) => impl_downcast!(as_multi_line_string),
                MultiPolygon(_) => impl_downcast!(as_multi_polygon),
                GeometryCollection(_) => impl_downcast!(as_geometry_collection),
                Rect(_) => impl_downcast!(as_rect),
                Geometry(_) => impl_downcast!(as_geometry),
            };
            Ok(Self(result))
        } else {
            Err(GeoArrowError::General(format!("Handling multiple geometry types in `from_geoarrow_chunks` not yet implemented. Received {:?}", data_types)))
        }
    }

    pub fn inner(&self) -> &Arc<dyn ChunkedNativeArray> {
        &self.0
    }

    pub fn into_inner(self) -> Arc<dyn ChunkedNativeArray> {
        self.0
    }
}
