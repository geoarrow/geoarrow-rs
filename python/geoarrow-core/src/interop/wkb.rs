use std::sync::Arc;

use geoarrow_array::cast::{
    AsGeoArrowArray, from_wkb as _from_wkb, to_wkb as _to_wkb, to_wkb_view,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayIterator};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{GeoArrowType, GeometryType, WkbType};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_geoarrow::data_type::PyGeoType;
use pyo3_geoarrow::input::AnyGeoArray;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrayReader};

use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(signature = (input, to_type = None))]
pub fn from_wkb(
    py: Python,
    input: AnyGeoArray,
    to_type: Option<PyGeoType>,
) -> PyGeoArrowResult<PyObject> {
    let input_metadata = input.data_type().metadata().clone();
    let to_type = to_type
        .map(|x| x.into_inner())
        .unwrap_or(GeometryType::new(input_metadata).into());
    match input {
        AnyGeoArray::Array(array) => {
            let out = impl_from_wkb(array.into_inner(), to_type)?;
            Ok(PyGeoArray::new(out).into_py_any(py)?)
        }
        AnyGeoArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let output_type = to_type.clone();
            let iter = reader
                .into_iter()
                .map(move |array| impl_from_wkb(array?, to_type.clone()));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_type));
            Ok(PyGeoArrayReader::new(output_reader).into_py_any(py)?)
        }
    }
}

fn impl_from_wkb(
    input_array: Arc<dyn GeoArrowArray>,
    to_type: GeoArrowType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let out = match input_array.data_type() {
        GeoArrowType::Wkb(_) => _from_wkb(input_array.as_wkb::<i32>(), to_type)?,
        GeoArrowType::LargeWkb(_) => _from_wkb(input_array.as_wkb::<i64>(), to_type)?,
        GeoArrowType::WkbView(_) => _from_wkb(input_array.as_wkb_view(), to_type)?,
        typ => {
            return Err(GeoArrowError::IncorrectGeometryType(format!(
                "Expected a WKB array in from_wkb, got {:?}",
                typ
            )));
        }
    };
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ToWkbType {
    #[default]
    Wkb,
    LargeWkb,
    WkbView,
}

impl<'a> FromPyObject<'a> for ToWkbType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "wkb" => Ok(Self::Wkb),
            "large_wkb" => Ok(Self::LargeWkb),
            "wkb_view" => Ok(Self::WkbView),
            _ => Err(PyValueError::new_err(
                "Unexpected wkb output type: should be one of 'wkb', 'large_wkb', or 'wkb_view'",
            )),
        }
    }
}

#[pyfunction]
#[pyo3(signature = (input, wkb_type = None))]
pub(crate) fn to_wkb(
    py: Python,
    input: AnyGeoArray,
    wkb_type: Option<ToWkbType>,
) -> PyGeoArrowResult<PyObject> {
    let wkb_type = wkb_type.unwrap_or_default();
    match input {
        AnyGeoArray::Array(array) => {
            let out = impl_to_wkb(array.into_inner(), wkb_type)?;
            Ok(PyGeoArray::new(out).into_py_any(py)?)
        }
        AnyGeoArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let input_metadata = reader.data_type().metadata().clone();
            let output_type = match wkb_type {
                ToWkbType::Wkb => GeoArrowType::Wkb(WkbType::new(input_metadata)),
                ToWkbType::LargeWkb => GeoArrowType::LargeWkb(WkbType::new(input_metadata)),
                ToWkbType::WkbView => GeoArrowType::WkbView(WkbType::new(input_metadata)),
            };
            let iter = reader
                .into_iter()
                .map(move |array| impl_to_wkb(array?, wkb_type));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_type));
            Ok(PyGeoArrayReader::new(output_reader).into_py_any(py)?)
        }
    }
}

fn impl_to_wkb(
    input_array: Arc<dyn GeoArrowArray>,
    wkb_type: ToWkbType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    match wkb_type {
        ToWkbType::Wkb => Ok(Arc::new(_to_wkb::<i32>(&input_array)?)),
        ToWkbType::LargeWkb => Ok(Arc::new(_to_wkb::<i64>(&input_array)?)),
        ToWkbType::WkbView => Ok(Arc::new(to_wkb_view(&input_array)?)),
    }
}
