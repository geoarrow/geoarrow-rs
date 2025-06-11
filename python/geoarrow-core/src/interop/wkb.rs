use std::sync::Arc;

use geoarrow_array::cast::{AsGeoArrowArray, from_wkb as _from_wkb};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayIterator};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{GeoArrowType, GeometryType};
use pyo3::IntoPyObjectExt;
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

// #[pyfunction]
// pub fn to_wkb(py: Python, input: AnyNativeInput) -> PyGeoArrowResult<PyObject> {
//     match input {
//         AnyNativeInput::Array(arr) => {
//             let wkb_arr = _to_wkb::<i32>(arr.as_ref());
//             let field = wkb_arr.extension_field();
//             Ok(PyArray::new(wkb_arr.into_array_ref(), field)
//                 .to_arro3(py)?
//                 .unbind())
//         }
//         AnyNativeInput::Chunked(s) => {
//             let out = s.as_ref().to_wkb::<i32>();
//             let field = out.extension_field();
//             Ok(PyChunkedArray::try_new(out.array_refs(), field)?
//                 .to_arro3(py)?
//                 .unbind())
//         }
//     }
// }
