use crate::array::*;
use crate::ffi::to_py_array;
use arrow2::array::Array;
use pyo3::prelude::*;

macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the length of a Line
            pub fn euclidean_length(&self, py: Python) -> PyResult<PyObject> {
                use geoarrow::algorithm::geo::EuclideanLength;
                let result =
                    py.allow_threads(|| EuclideanLength::euclidean_length(&self.0).to_boxed());
                to_py_array(py, result)
            }
        }
    };
}

impl_euclidean_length!(PointArray);
impl_euclidean_length!(MultiPointArray);
impl_euclidean_length!(LineStringArray);
impl_euclidean_length!(MultiLineStringArray);
