use geoarrow::array::{from_arrow_array, CoordType};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::wkb::{from_wkb as _from_wkb, to_wkb as _to_wkb};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::array::*;
use crate::ffi::from_python::import_arrow_c_array;
use crate::ffi::to_python::geometry_array_to_pyobject;

/// Convert an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.
#[pyfunction]
pub fn from_wkb(ob: &PyAny) -> PyResult<PyObject> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = from_arrow_array(&array, &field).unwrap();

    let geo_array = match array.data_type() {
        GeoDataType::WKB => _from_wkb(
            array
                .as_any()
                .downcast_ref::<geoarrow::array::WKBArray<i32>>()
                .unwrap(),
            false,
            CoordType::Interleaved,
        )
        .unwrap(),
        GeoDataType::LargeWKB => _from_wkb(
            array
                .as_any()
                .downcast_ref::<geoarrow::array::WKBArray<i64>>()
                .unwrap(),
            false,
            CoordType::Interleaved,
        )
        .unwrap(),
        other => {
            return Err(PyTypeError::new_err(format!(
                "Unexpected array type {:?}",
                other
            )))
        }
    };
    Python::with_gil(|py| geometry_array_to_pyobject(py, geo_array))
}

/// Convert a GeoArrow-native geometry array to a WKBArray.
#[pyfunction]
pub fn to_wkb(ob: &PyAny) -> PyResult<WKBArray> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = from_arrow_array(&array, &field).unwrap();
    Ok(WKBArray(_to_wkb(array.as_ref())))
}
