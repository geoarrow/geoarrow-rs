use arrow_schema::Schema;
use geoarrow_array::{GeoArrowArrayIterator, WrapArray};
use geoarrow_schema::GeoArrowType;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrayReader, PyGeoArrowResult};

#[pyfunction]
#[pyo3(signature = (input, *, name = None))]
pub fn geometry_col<'py>(
    py: Python<'py>,
    input: AnyRecordBatch,
    name: Option<PyBackedStr>,
) -> PyGeoArrowResult<Bound<'py, PyAny>> {
    let schema = input.schema()?;

    let (geom_index, geom_type) = if let Some(name) = name {
        let (idx, field) = schema
            .column_with_name(&name)
            .ok_or(PyNotImplementedError::new_err(format!(
                "Column name {name} not found"
            )))?;

        let geom_type = GeoArrowType::from_arrow_field(field)?;
        (idx, geom_type)
    } else {
        let geom_cols = geometry_columns(schema.as_ref());
        if geom_cols.len() == 1 {
            geom_cols.into_iter().next().unwrap()
        } else {
            return Err(PyNotImplementedError::new_err(
                "Accessing from multiple geometry columns not yet supported",
            )
            .into());
        }
    };

    match input {
        AnyRecordBatch::RecordBatch(batch) => {
            let geo_array = geom_type.wrap_array(batch.as_ref().column(geom_index).as_ref())?;
            Ok(PyGeoArray::new(geo_array).into_geoarrow_py(py)?)
        }
        AnyRecordBatch::Stream(stream) => {
            let reader = stream.into_reader()?;
            let output_geo_type = geom_type.clone();
            let iter = reader
                .into_iter()
                .map(move |batch| geom_type.wrap_array(batch?.column(geom_index).as_ref()));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_geo_type));

            Ok(PyGeoArrayReader::new(output_reader).into_geoarrow_py(py)?)
        }
    }
}

fn geometry_columns(schema: &Schema) -> Vec<(usize, GeoArrowType)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if let Ok(geom_type) = GeoArrowType::from_extension_field(field) {
                Some((idx, geom_type))
            } else {
                None
            }
        })
        .collect()
}
