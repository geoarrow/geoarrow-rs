use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::array::geometry_to_pyobject;
use crate::scalar::*;
use pyo3::exceptions::PyIndexError;
use pyo3::prelude::*;

macro_rules! impl_getitem {
    ($struct_name:ident, $return_type:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Access the item at a given index
            pub fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<$return_type>> {
                // Handle negative indexes from the end
                let i = if i < 0 {
                    let i = self.0.len() as isize + i;
                    if i < 0 {
                        return Err(PyIndexError::new_err("Index out of range").into());
                    }
                    i as usize
                } else {
                    i as usize
                };
                if i >= self.0.len() {
                    return Err(PyIndexError::new_err("Index out of range").into());
                }
                Ok(self.0.get(i).map(|geom| $return_type(geom.into())))
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
    pub fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyObject>> {
        // Handle negative indexes from the end
        let i = if i < 0 {
            let i = self.0.len() as isize + i;
            if i < 0 {
                return Err(PyIndexError::new_err("Index out of range").into());
            }
            i as usize
        } else {
            i as usize
        };
        if i >= self.0.len() {
            return Err(PyIndexError::new_err("Index out of range").into());
        }
        let geom = self.0.get(i);
        Ok(Python::with_gil(|py| {
            geom.map(|g| geometry_to_pyobject(py, g))
        }))
    }
}
