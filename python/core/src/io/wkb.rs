use geoarrow::array::CoordType;
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::wkb::{from_wkb as _from_wkb, to_wkb as _to_wkb};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::array::*;
use crate::ffi::from_python::{convert_to_geometry_array, import_arrow_c_array};

/// Convert an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.
#[pyfunction]
pub fn from_wkb(ob: &PyAny) -> PyResult<PyObject> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = convert_to_geometry_array(&array, &field).unwrap();

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
    Python::with_gil(|py| {
        let downcasted = match geo_array.data_type() {
            GeoDataType::Point(_) => PointArray(
                geo_array
                    .as_any()
                    .downcast_ref::<geoarrow::array::PointArray>()
                    .unwrap()
                    .clone(),
            )
            .into_py(py),
            GeoDataType::LineString(_) => LineStringArray(
                geo_array
                    .as_any()
                    .downcast_ref::<geoarrow::array::LineStringArray<i32>>()
                    .unwrap()
                    .clone(),
            )
            .into_py(py),
            GeoDataType::Polygon(_) => PolygonArray(
                geo_array
                    .as_any()
                    .downcast_ref::<geoarrow::array::PolygonArray<i32>>()
                    .unwrap()
                    .clone(),
            )
            .into_py(py),
            GeoDataType::MultiPoint(_) => MultiPointArray(
                geo_array
                    .as_any()
                    .downcast_ref::<geoarrow::array::MultiPointArray<i32>>()
                    .unwrap()
                    .clone(),
            )
            .into_py(py),
            GeoDataType::MultiLineString(_) => MultiLineStringArray(
                geo_array
                    .as_any()
                    .downcast_ref::<geoarrow::array::MultiLineStringArray<i32>>()
                    .unwrap()
                    .clone(),
            )
            .into_py(py),
            GeoDataType::MultiPolygon(_) => MultiPolygonArray(
                geo_array
                    .as_any()
                    .downcast_ref::<geoarrow::array::MultiPolygonArray<i32>>()
                    .unwrap()
                    .clone(),
            )
            .into_py(py),
            other => {
                return Err(PyTypeError::new_err(format!(
                    "Unexpected parsed geometry array type {:?}",
                    other
                )))
            }
        };

        Ok(downcasted)
    })
}

/// Convert a GeoArrow-native geometry array to a WKBArray.
#[pyfunction]
pub fn to_wkb(ob: &PyAny) -> PyResult<WKBArray> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = convert_to_geometry_array(&array, &field).unwrap();
    Ok(WKBArray(_to_wkb(array.as_ref())))
}
