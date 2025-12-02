use std::sync::Arc;

use geoarrow_array::cast::{
    AsGeoArrowArray, from_wkt as _from_wkt, to_wkt as _to_wkt, to_wkt_view,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayIterator};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{GeoArrowType, GeometryType, WktType};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_geoarrow::data_type::PyGeoType;
use pyo3_geoarrow::input::AnyGeoArray;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrayReader};

use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(signature = (input, to_type = None))]
pub fn from_wkt(
    py: Python,
    input: AnyGeoArray,
    to_type: Option<PyGeoType>,
) -> PyGeoArrowResult<Py<PyAny>> {
    let input_metadata = input.data_type().metadata().clone();
    let to_type = to_type
        .map(|x| x.into_inner())
        .unwrap_or(GeometryType::new(input_metadata).into());
    match input {
        AnyGeoArray::Array(array) => {
            let out = impl_from_wkt(array.into_inner(), to_type)?;
            Ok(PyGeoArray::new(out).into_py_any(py)?)
        }
        AnyGeoArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let output_type = to_type.clone();
            let iter = reader
                .into_iter()
                .map(move |array| impl_from_wkt(array?, to_type.clone()));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_type));
            Ok(PyGeoArrayReader::new(output_reader).into_py_any(py)?)
        }
    }
}

fn impl_from_wkt(
    input_array: Arc<dyn GeoArrowArray>,
    to_type: GeoArrowType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let out = match input_array.data_type() {
        GeoArrowType::Wkt(_) => _from_wkt(input_array.as_wkt::<i32>(), to_type)?,
        GeoArrowType::LargeWkt(_) => _from_wkt(input_array.as_wkt::<i64>(), to_type)?,
        GeoArrowType::WktView(_) => _from_wkt(input_array.as_wkt_view(), to_type)?,
        typ => {
            return Err(GeoArrowError::IncorrectGeometryType(format!(
                "Expected a WKT array in from_wkt, got {typ:?}",
            )));
        }
    };
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ToWktType {
    #[default]
    Wkt,
    LargeWkt,
    WktView,
}

impl<'a, 'py> FromPyObject<'a, 'py> for ToWktType {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let ob = ob.as_ref().bind(ob.py());
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "wkt" => Ok(Self::Wkt),
            "largewkt" | "large_wkt" | "large-wkt" => Ok(Self::LargeWkt),
            "wktview" | "wkt_view" | "wkt-view" => Ok(Self::WktView),
            _ => Err(PyValueError::new_err(
                "Unexpected wkt output type: should be one of 'wkt', 'large_wkt', or 'wkt_view'",
            )),
        }
    }
}

#[pyfunction]
#[pyo3(signature = (input, wkt_type = None))]
pub(crate) fn to_wkt(
    py: Python,
    input: AnyGeoArray,
    wkt_type: Option<ToWktType>,
) -> PyGeoArrowResult<Py<PyAny>> {
    let wkt_type = wkt_type.unwrap_or_default();
    match input {
        AnyGeoArray::Array(array) => {
            let out = impl_to_wkt(array.into_inner(), wkt_type)?;
            Ok(PyGeoArray::new(out).into_py_any(py)?)
        }
        AnyGeoArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let input_metadata = reader.data_type().metadata().clone();
            let output_type = match wkt_type {
                ToWktType::Wkt => GeoArrowType::Wkt(WktType::new(input_metadata)),
                ToWktType::LargeWkt => GeoArrowType::LargeWkt(WktType::new(input_metadata)),
                ToWktType::WktView => GeoArrowType::WktView(WktType::new(input_metadata)),
            };
            let iter = reader
                .into_iter()
                .map(move |array| impl_to_wkt(array?, wkt_type));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_type));
            Ok(PyGeoArrayReader::new(output_reader).into_py_any(py)?)
        }
    }
}

fn impl_to_wkt(
    input_array: Arc<dyn GeoArrowArray>,
    wkt_type: ToWktType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    match wkt_type {
        ToWktType::Wkt => Ok(Arc::new(_to_wkt::<i32>(&input_array)?)),
        ToWktType::LargeWkt => Ok(Arc::new(_to_wkt::<i64>(&input_array)?)),
        ToWktType::WktView => Ok(Arc::new(to_wkt_view(&input_array)?)),
    }
}
