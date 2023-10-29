use crate::array::*;
use crate::ffi::to_py_array;
use pyo3::prelude::*;

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            pub fn area(&self, py: Python) -> PyResult<PyObject> {
                use geoarrow::algorithm::geo::Area;
                let result = py.allow_threads(|| Area::unsigned_area(&self.0).to_boxed());
                to_py_array(py, result)
            }

            /// Signed planar area of a geometry.
            pub fn signed_area(&self, py: Python) -> PyResult<PyObject> {
                use geoarrow::algorithm::geo::Area;
                let result = py.allow_threads(|| Area::signed_area(&self.0).to_boxed());
                to_py_array(py, result)
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);
// impl_area!(GeometryArray);
