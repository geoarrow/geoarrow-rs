use geoarrow::array::AsGeometryArray;
use geoarrow::array::{from_arrow_array, CoordType};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::wkb::{from_wkb as _from_wkb, to_wkb as _to_wkb};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

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
            None,
        )
        .unwrap(),
        GeoDataType::LargeWKB => _from_wkb(
            array
                .as_any()
                .downcast_ref::<geoarrow::array::WKBArray<i64>>()
                .unwrap(),
            false,
            CoordType::Interleaved,
            None,
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

macro_rules! impl_from_wkb {
    ($array:ty, $builder:ty) => {
        #[pymethods]
        impl $array {
            #[classmethod]
            pub fn from_wkb(_cls: &PyType, ob: &PyAny) -> PyResult<$array> {
                let (array, field) = import_arrow_c_array(ob)?;
                let array = from_arrow_array(&array, &field).unwrap();
                let ref_array = array.as_ref();
                match array.data_type() {
                    GeoDataType::WKB => {
                        let wkb_arr = ref_array.as_wkb();
                        let builder = <$builder>::from_wkb(
                            wkb_arr.iter().collect::<Vec<_>>().as_slice(),
                            Default::default(),
                        )
                        .unwrap();
                        Ok(builder.finish().into())
                    }
                    GeoDataType::LargeWKB => {
                        let wkb_arr = ref_array.as_large_wkb();
                        let builder = <$builder>::from_wkb(
                            wkb_arr.iter().collect::<Vec<_>>().as_slice(),
                            Default::default(),
                        )
                        .unwrap();
                        Ok(builder.finish().into())
                    }
                    other => Err(PyTypeError::new_err(format!(
                        "Unexpected array type {:?}",
                        other
                    ))),
                }
            }
        }
    };
}

impl_from_wkb!(PointArray, geoarrow::array::PointBuilder);
impl_from_wkb!(LineStringArray, geoarrow::array::LineStringBuilder<i32>);
impl_from_wkb!(PolygonArray, geoarrow::array::PolygonBuilder<i32>);
impl_from_wkb!(MultiPointArray, geoarrow::array::MultiPointBuilder<i32>);
impl_from_wkb!(
    MultiLineStringArray,
    geoarrow::array::MultiLineStringBuilder<i32>
);
impl_from_wkb!(MultiPolygonArray, geoarrow::array::MultiPolygonBuilder<i32>);

// TODO: handle extra argument
// impl_from_wkb!(MixedGeometryArray, geoarrow::array::MixedGeometryBuilder<i32>);
// impl_from_wkb!(GeometryCollectionArray, geoarrow::array::GeometryCollectionBuilder<i32>);

macro_rules! impl_to_wkb {
    ($array:ty) => {
        #[pymethods]
        impl $array {
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
