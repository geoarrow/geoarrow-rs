use arrow_schema::Schema;
use geoarrow_array::{GeoArrowArrayIterator, WrapArray};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowError;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_geoarrow::input::AnyGeoArray;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrayReader, PyGeoArrowResult};

#[pyfunction]
#[pyo3(signature = (input, *, name = None))]
pub fn geometry_col<'py>(
    py: Python<'py>,
    input: Bound<'py, PyAny>,
    name: Option<PyBackedStr>,
) -> PyGeoArrowResult<Bound<'py, PyAny>> {
    // If the input is already a GeoArray, just return it
    if let Ok(input) = input.extract::<AnyGeoArray>() {
        match input {
            AnyGeoArray::Array(array) => {
                return Ok(array.into_bound_py_any(py)?);
            }
            AnyGeoArray::Stream(stream) => {
                return Ok(stream.into_bound_py_any(py)?);
            }
        }
    }

    // Otherwise, assume it's a RecordBatch or RecordBatchStream
    let input = input.extract::<AnyRecordBatch>()?;

    let schema = input.schema()?;

    let (geom_index, geom_type) = if let Some(name) = name {
        let (idx, field) = schema
            .column_with_name(&name)
            .ok_or(PyIndexError::new_err(format!(
                "Column name {name} not found"
            )))?;

        let geom_type = GeoArrowType::from_arrow_field(field)
            .and_then(|f| f.ok_or(GeoArrowError::NotGeoArrowArray))?;
        (idx, geom_type)
    } else {
        let geom_cols = geometry_columns(schema.as_ref());
        if geom_cols.is_empty() {
            return Err(PyValueError::new_err("No geometry columns found").into());
        } else if geom_cols.len() == 1 {
            geom_cols.into_iter().next().unwrap()
        } else {
            return Err(PyValueError::new_err(
                "Multiple geometry columns: 'name' must be provided.",
            )
            .into());
        }
    };

    match input {
        AnyRecordBatch::RecordBatch(batch) => {
            let geo_array = geom_type.wrap_array(batch.as_ref().column(geom_index).as_ref())?;
            Ok(PyGeoArray::new(geo_array).into_bound_py_any(py)?)
        }
        AnyRecordBatch::Stream(stream) => {
            let reader = stream.into_reader()?;
            let output_geo_type = geom_type.clone();
            let iter = reader
                .into_iter()
                .map(move |batch| geom_type.wrap_array(batch?.column(geom_index).as_ref()));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_geo_type));

            Ok(PyGeoArrayReader::new(output_reader).into_bound_py_any(py)?)
        }
    }
}

fn geometry_columns(schema: &Schema) -> Vec<(usize, GeoArrowType)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if let Ok(geom_type) = GeoArrowType::from_extension_field(field)
                .and_then(|f| f.ok_or(GeoArrowError::NotGeoArrowArray))
            {
                Some((idx, geom_type))
            } else {
                None
            }
        })
        .collect()
}
