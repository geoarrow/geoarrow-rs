use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::cast::AsArray;
use geoarrow::array::CoordType;
use geoarrow::io::geozero::FromWKT;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::array::*;
use crate::ffi::from_python::import_arrow_c_array;
use crate::ffi::to_python::geometry_array_to_pyobject;

/// Convert an Arrow StringArray from WKT to its GeoArrow-native counterpart.
#[pyfunction]
pub fn from_wkt(ob: &PyAny) -> PyResult<PyObject> {
    let (array, _field) = import_arrow_c_array(ob)?;
    let geo_array: Arc<dyn GeometryArrayTrait> = match array.data_type() {
        DataType::Utf8 => {
            FromWKT::from_wkt(array.as_string::<i32>(), CoordType::Interleaved).unwrap()
        }
        DataType::LargeUtf8 => {
            FromWKT::from_wkt(array.as_string::<i64>(), CoordType::Interleaved).unwrap()
        }
        other => {
            return Err(PyTypeError::new_err(format!(
                "Unexpected array type {:?}",
                other
            )))
        }
    };
    Python::with_gil(|py| geometry_array_to_pyobject(py, geo_array))
}

macro_rules! impl_from_wkt {
    ($py_array:ty, $geoarrow_array:ty) => {
        #[pymethods]
        impl $py_array {
            /// Parse from WKT
            #[classmethod]
            pub fn from_wkt(_cls: &PyType, ob: &PyAny) -> PyResult<$py_array> {
                let (array, _field) = import_arrow_c_array(ob)?;
                match array.data_type() {
                    DataType::Utf8 => Ok(<$geoarrow_array>::from_wkt(
                        array.as_string::<i32>(),
                        CoordType::Interleaved,
                    )
                    .unwrap()
                    .into()),
                    DataType::LargeUtf8 => Ok(<$geoarrow_array>::from_wkt(
                        array.as_string::<i64>(),
                        CoordType::Interleaved,
                    )
                    .unwrap()
                    .into()),
                    other => Err(PyTypeError::new_err(format!(
                        "Unexpected array type {:?}",
                        other
                    ))),
                }
            }
        }
    };
}

impl_from_wkt!(MixedGeometryArray, geoarrow::array::MixedGeometryArray<i32>);
impl_from_wkt!(
    GeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32>
);
