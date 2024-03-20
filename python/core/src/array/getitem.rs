use crate::array::*;
use crate::ffi::to_python::array::geometry_to_pyobject;
use crate::scalar::*;
use geoarrow::trait_::GeometryArrayAccessor;
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

macro_rules! impl_getitem {
    ($struct_name:ident, $return_type:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Access the item at a given index
            pub fn __getitem__(&self, key: isize) -> Option<$return_type> {
                // Handle negative indexes from the end
                let index = if key < 0 {
                    self.0.len() + key as usize
                } else {
                    key as usize
                };
                self.0.get(index).map(|geom| $return_type(geom.into()))
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
impl_getitem!(GeometryCollectionArray, GeometryCollection);
impl_getitem!(WKBArray, WKB);
impl_getitem!(RectArray, Rect);

#[pymethods]
impl MixedGeometryArray {
    /// Access the item at a given index
    pub fn __getitem__(&self, key: isize) -> Option<PyObject> {
        // Handle negative indexes from the end
        let index = if key < 0 {
            self.0.len() + key as usize
        } else {
            key as usize
        };
        let geom = self.0.get(index);
        Python::with_gil(|py| geom.map(|g| geometry_to_pyobject(py, g)))
    }
}
