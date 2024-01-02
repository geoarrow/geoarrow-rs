use std::sync::Arc;

use geoarrow::array::{from_arrow_array, AsGeometryArray, CoordType};
use geoarrow::datatypes::GeoDataType;
use geoarrow::error::GeoArrowError;
use geoarrow::io::wkb::{to_wkb as _to_wkb, FromWKB};
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use crate::ffi::to_python::geometry_array_to_pyobject;

/// Parse an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.
///
/// This expects ISO-formatted WKB geometries.
///
/// Args:
///     input: An Arrow array of Binary type holding WKB-formatted geometries.
///
/// Returns:
///     A GeoArrow-native geometry array
#[pyfunction]
pub fn from_wkb(input: &PyAny) -> PyGeoArrowResult<PyObject> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    let ref_array = array.as_ref();
    let geo_array: Arc<dyn GeometryArrayTrait> = match array.data_type() {
        GeoDataType::WKB => FromWKB::from_wkb(ref_array.as_wkb(), CoordType::Interleaved)?,
        GeoDataType::LargeWKB => {
            FromWKB::from_wkb(ref_array.as_large_wkb(), CoordType::Interleaved)?
        }
        other => {
            return Err(GeoArrowError::IncorrectType(
                format!("Unexpected array type {:?}", other).into(),
            )
            .into())
        }
    };
    Python::with_gil(|py| geometry_array_to_pyobject(py, geo_array))
}

/// Encode a GeoArrow-native geometry array to a WKBArray, holding ISO-formatted WKB geometries.
///
/// Args:
///     input: A GeoArrow-native geometry array
///
/// Returns:
///     An array with WKB-formatted geometries
#[pyfunction]
pub fn to_wkb(input: &PyAny) -> PyGeoArrowResult<WKBArray> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(WKBArray(_to_wkb(array.as_ref())))
}

macro_rules! impl_from_wkb {
    ($py_array:ty, $geoarrow_array:ty) => {
        #[pymethods]
        impl $py_array {
            /// Parse an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.
            ///
            /// This expects ISO-formatted WKB geometries.
            ///
            /// Args:
            ///     input: An Arrow array of Binary type holding WKB-formatted geometries.
            ///
            /// Returns:
            ///     A GeoArrow-native geometry array
            #[classmethod]
            pub fn from_wkb(_cls: &PyType, input: &PyAny) -> PyGeoArrowResult<$py_array> {
                let (array, field) = import_arrow_c_array(input)?;
                let array = from_arrow_array(&array, &field)?;
                let ref_array = array.as_ref();
                match array.data_type() {
                    GeoDataType::WKB => Ok(<$geoarrow_array>::from_wkb(
                        ref_array.as_wkb(),
                        CoordType::Interleaved,
                    )?
                    .into()),
                    GeoDataType::LargeWKB => Ok(<$geoarrow_array>::from_wkb(
                        ref_array.as_large_wkb(),
                        CoordType::Interleaved,
                    )?
                    .into()),
                    other => Err(PyTypeError::new_err(format!(
                        "Unexpected array type {:?}",
                        other
                    ))
                    .into()),
                }
            }
        }
    };
}

impl_from_wkb!(PointArray, geoarrow::array::PointArray);
impl_from_wkb!(LineStringArray, geoarrow::array::LineStringArray<i32>);
impl_from_wkb!(PolygonArray, geoarrow::array::PolygonArray<i32>);
impl_from_wkb!(MultiPointArray, geoarrow::array::MultiPointArray<i32>);
impl_from_wkb!(
    MultiLineStringArray,
    geoarrow::array::MultiLineStringArray<i32>
);
impl_from_wkb!(MultiPolygonArray, geoarrow::array::MultiPolygonArray<i32>);
impl_from_wkb!(MixedGeometryArray, geoarrow::array::MixedGeometryArray<i32>);
impl_from_wkb!(
    GeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32>
);

// macro_rules! impl_from_wkb_chunked {
//     ($py_array:ident) => {
//         #[pymethods]
//         impl $py_array {
//             /// Parse from WKB
//             #[classmethod]
//             pub fn from_wkb(_cls: &PyType, ob: &PyAny) -> PyResult<$py_array> {
//                 // TODO: need to implement FFI reading of chunked arrays
//                 // FromWKB::from_wkb(&self.0, CoordType::Interleaved)
//                 //     .unwrap()
//                 //     .into()
//             }
//         }
//     };
// }

macro_rules! impl_to_wkb {
    ($array:ty) => {
        #[pymethods]
        impl $array {
            /// Encode a GeoArrow-native geometry array to a WKBArray, holding ISO-formatted WKB
            /// geometries.
            ///
            /// Returns:
            ///     An array with WKB-formatted geometries
            pub fn to_wkb(&self) -> PyResult<WKBArray> {
                let wkb_arr = geoarrow::array::WKBArray::<i32>::from(&self.0);
                Ok(wkb_arr.into())
            }
        }
    };
}

impl_to_wkb!(PointArray);
impl_to_wkb!(LineStringArray);
impl_to_wkb!(PolygonArray);
impl_to_wkb!(MultiPointArray);
impl_to_wkb!(MultiLineStringArray);
impl_to_wkb!(MultiPolygonArray);
impl_to_wkb!(MixedGeometryArray);
impl_to_wkb!(GeometryCollectionArray);
