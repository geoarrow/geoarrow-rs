use std::sync::Arc;

use geoarrow::array::{from_arrow_array, AsGeometryArray, CoordType};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::geozero::FromEWKB;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::array::*;
use crate::ffi::from_python::import_arrow_c_array;
use crate::ffi::to_python::geometry_array_to_pyobject;

/// Convert an Arrow BinaryArray from EWKB to its GeoArrow-native counterpart.
#[pyfunction]
pub fn from_ewkb(ob: &PyAny) -> PyResult<PyObject> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = from_arrow_array(&array, &field).unwrap();
    let ref_array = array.as_ref();
    let geo_array: Arc<dyn GeometryArrayTrait> = match array.data_type() {
        GeoDataType::WKB => {
            FromEWKB::from_ewkb(ref_array.as_wkb(), CoordType::Interleaved).unwrap()
        }
        GeoDataType::LargeWKB => {
            FromEWKB::from_ewkb(ref_array.as_large_wkb(), CoordType::Interleaved).unwrap()
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

macro_rules! impl_from_ewkb {
    ($py_array:ty, $geoarrow_array:ty) => {
        #[pymethods]
        impl $py_array {
            /// Parse from EWKB
            #[classmethod]
            pub fn from_ewkb(_cls: &PyType, ob: &PyAny) -> PyResult<$py_array> {
                let (array, field) = import_arrow_c_array(ob)?;
                let array = from_arrow_array(&array, &field).unwrap();
                let ref_array = array.as_ref();
                match array.data_type() {
                    GeoDataType::WKB => Ok(<$geoarrow_array>::from_ewkb(
                        ref_array.as_wkb(),
                        CoordType::Interleaved,
                    )
                    .unwrap()
                    .into()),
                    GeoDataType::LargeWKB => Ok(<$geoarrow_array>::from_ewkb(
                        ref_array.as_large_wkb(),
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

impl_from_ewkb!(MixedGeometryArray, geoarrow::array::MixedGeometryArray<i32>);
impl_from_ewkb!(
    GeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32>
);
