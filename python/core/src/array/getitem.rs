use crate::array::*;
use crate::scalar::*;
use geoarrow::trait_::GeometryArrayAccessor;
use pyo3::prelude::*;

macro_rules! impl_getitem {
    ($struct_name:ident, $return_type:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Access the item at a given index
            pub fn __getitem__(&self, key: usize) -> Option<$return_type> {
                self.0.get(key).map(|geom| $return_type(geom.into()))
            }
        }
    };
}

impl_getitem!(PointArray, Point);
impl_getitem!(LineStringArray, LineString);
impl_getitem!(PolygonArray, Polygon);
impl_getitem!(MultiPointArray, MultiPoint);
impl_getitem!(MultiLineStringArray, MultiLineString);
impl_getitem!(MultiPolygonArray, MultiPolygon);
