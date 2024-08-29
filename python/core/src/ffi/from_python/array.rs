use crate::array::*;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::{PyAny, PyResult};
use pyo3_arrow::PyArray;

// impl<'a> FromPyObject<'a> for GeometryArray {
//     fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
//         let (array, field) = ob.extract::<PyArray>()?.into_inner();
//         Ok(Self(
//             from_arrow_array(&array, &field)
//                 .map_err(|err| PyTypeError::new_err(err.to_string()))?,
//         ))
//     }
// }

macro_rules! impl_from_py_object {
    ($struct_name:ident, $geoarrow_arr:ty) => {
        impl<'a> FromPyObject<'a> for $struct_name {
            fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
                let (array, _field) = ob.extract::<PyArray>()?.into_inner();
                let geo_array = <$geoarrow_arr>::try_from(array.as_ref())
                    .map_err(|err| PyTypeError::new_err(err.to_string()))?;
                Ok(geo_array.into())
            }
        }
    };
}

impl_from_py_object!(WKBArray, geoarrow::array::WKBArray<i32>);
impl_from_py_object!(PointArray, geoarrow::array::PointArray<2>);
impl_from_py_object!(LineStringArray, geoarrow::array::LineStringArray<i32, 2>);
impl_from_py_object!(PolygonArray, geoarrow::array::PolygonArray<i32, 2>);
impl_from_py_object!(MultiPointArray, geoarrow::array::MultiPointArray<i32, 2>);
impl_from_py_object!(
    MultiLineStringArray,
    geoarrow::array::MultiLineStringArray<i32, 2>
);
impl_from_py_object!(MultiPolygonArray, geoarrow::array::MultiPolygonArray<i32, 2>);
impl_from_py_object!(MixedGeometryArray, geoarrow::array::MixedGeometryArray<i32, 2>);
impl_from_py_object!(RectArray, geoarrow::array::RectArray<2>);
impl_from_py_object!(
    GeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32, 2>
);

macro_rules! impl_from_arrow {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Construct this object from existing Arrow data
            ///
            /// Args:
            ///     input: Arrow array to use for constructing this object
            ///
            /// Returns:
            ///     Self
            #[classmethod]
            pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
                input.extract()
            }
        }
    };
}

impl_from_arrow!(WKBArray);
impl_from_arrow!(PointArray);
impl_from_arrow!(LineStringArray);
impl_from_arrow!(PolygonArray);
impl_from_arrow!(MultiPointArray);
impl_from_arrow!(MultiLineStringArray);
impl_from_arrow!(MultiPolygonArray);
impl_from_arrow!(MixedGeometryArray);
impl_from_arrow!(RectArray);
impl_from_arrow!(GeometryCollectionArray);
