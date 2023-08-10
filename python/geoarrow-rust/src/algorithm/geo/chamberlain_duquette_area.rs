use crate::array::*;
use crate::ffi::to_py_array;
use arrow2::array::Array;
use pyo3::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_unsigned_area(&self, py: Python) -> PyResult<PyObject> {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                let result = py.allow_threads(|| {
                    ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0).to_boxed()
                });
                to_py_array(py, result)
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_signed_area(&self, py: Python) -> PyResult<PyObject> {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                let result = py.allow_threads(|| {
                    ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0).to_boxed()
                });
                to_py_array(py, result)
            }
        }
    };
}

impl_alg!(PointArray);
impl_alg!(LineStringArray);
impl_alg!(PolygonArray);
impl_alg!(MultiPointArray);
impl_alg!(MultiLineStringArray);
impl_alg!(MultiPolygonArray);
// impl_alg!(GeometryArray);
