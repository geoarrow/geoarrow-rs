use crate::array::*;
use crate::chunked_array::*;
use pyo3::prelude::*;

macro_rules! impl_bounding_rect {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return the bounding rectangle of a geometry
            pub fn bounding_rect(&self) -> RectArray {
                use geoarrow::algorithm::geo::BoundingRect;
                RectArray(BoundingRect::bounding_rect(&self.0))
            }
        }
    };
}

impl_bounding_rect!(PointArray);
impl_bounding_rect!(LineStringArray);
impl_bounding_rect!(PolygonArray);
impl_bounding_rect!(MultiPointArray);
impl_bounding_rect!(MultiLineStringArray);
impl_bounding_rect!(MultiPolygonArray);
impl_bounding_rect!(MixedGeometryArray);
impl_bounding_rect!(GeometryCollectionArray);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return the bounding rectangle of a geometry
            pub fn bounding_rect(&self) -> PyResult<ChunkedRectArray> {
                use geoarrow::algorithm::geo::BoundingRect;
                Ok(ChunkedRectArray(
                    BoundingRect::bounding_rect(&self.0).unwrap(),
                ))
            }
        }
    };
}

impl_vector!(ChunkedPointArray);
impl_vector!(ChunkedLineStringArray);
impl_vector!(ChunkedPolygonArray);
impl_vector!(ChunkedMultiPointArray);
impl_vector!(ChunkedMultiLineStringArray);
impl_vector!(ChunkedMultiPolygonArray);
impl_vector!(ChunkedMixedGeometryArray);
impl_vector!(ChunkedGeometryCollectionArray);
