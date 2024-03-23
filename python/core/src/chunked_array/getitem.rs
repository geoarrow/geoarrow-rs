use crate::chunked_array::*;
use crate::ffi::to_python::array::geometry_to_pyobject;
use crate::scalar::*;
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

impl_getitem!(ChunkedPointArray, Point);
impl_getitem!(ChunkedLineStringArray, LineString);
impl_getitem!(ChunkedPolygonArray, Polygon);
impl_getitem!(ChunkedMultiPointArray, MultiPoint);
impl_getitem!(ChunkedMultiLineStringArray, MultiLineString);
impl_getitem!(ChunkedMultiPolygonArray, MultiPolygon);
impl_getitem!(ChunkedGeometryCollectionArray, GeometryCollection);
impl_getitem!(ChunkedWKBArray, WKB);
impl_getitem!(ChunkedRectArray, Rect);

#[pymethods]
impl ChunkedMixedGeometryArray {
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
