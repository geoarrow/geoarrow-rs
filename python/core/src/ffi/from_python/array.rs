use crate::array::*;
use crate::ffi::from_python::utils::import_arrow_c_array;
use crate::table::GeoTable;
use arrow::array::AsArray;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::{PyAny, PyResult};

macro_rules! impl_from_py_object {
    ($struct_name:ident, $geoarrow_arr:ty) => {
        impl<'a> FromPyObject<'a> for $struct_name {
            fn extract(ob: &'a PyAny) -> PyResult<Self> {
                let (array, _field) = import_arrow_c_array(ob)?;
                let geo_array = <$geoarrow_arr>::try_from(array.as_ref())
                    .map_err(|err| PyTypeError::new_err(err.to_string()))?;
                Ok(geo_array.into())
            }
        }
    };
}

impl_from_py_object!(WKBArray, geoarrow::array::WKBArray<i32>);
impl_from_py_object!(PointArray, geoarrow::array::PointArray);
impl_from_py_object!(LineStringArray, geoarrow::array::LineStringArray<i32>);
impl_from_py_object!(PolygonArray, geoarrow::array::PolygonArray<i32>);
impl_from_py_object!(MultiPointArray, geoarrow::array::MultiPointArray<i32>);
impl_from_py_object!(
    MultiLineStringArray,
    geoarrow::array::MultiLineStringArray<i32>
);
impl_from_py_object!(MultiPolygonArray, geoarrow::array::MultiPolygonArray<i32>);
impl_from_py_object!(MixedGeometryArray, geoarrow::array::MixedGeometryArray<i32>);
// impl_from_py_object!(RectArray);
impl_from_py_object!(
    GeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32>
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
            pub fn from_arrow(_cls: &PyType, input: &PyAny) -> PyResult<Self> {
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
// impl_from_arrow!(RectArray);
impl_from_arrow!(GeometryCollectionArray);
impl_from_arrow!(GeoTable);

macro_rules! impl_primitive {
    ($struct_name:ident) => {
        impl<'a> FromPyObject<'a> for $struct_name {
            fn extract(ob: &'a PyAny) -> PyResult<Self> {
                let (array, _field) = import_arrow_c_array(ob)?;
                let arr = array
                    .as_primitive_opt()
                    .ok_or(PyValueError::new_err("Unexpected type in arrow array"))?;
                Ok(Self(arr.clone()))
            }
        }
    };
}

impl_primitive!(UInt8Array);
impl_primitive!(UInt16Array);
impl_primitive!(UInt32Array);
impl_primitive!(UInt64Array);
impl_primitive!(Int8Array);
impl_primitive!(Int16Array);
impl_primitive!(Int32Array);
impl_primitive!(Int64Array);
impl_primitive!(Float16Array);
impl_primitive!(Float32Array);
impl_primitive!(Float64Array);
