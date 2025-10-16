use arrow_schema::Schema;
use geoarrow_array::{GeoArrowArrayIterator, WrapArray};
use geoarrow_schema::GeoArrowType;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrayReader, PyGeoArrowResult};

#[pyfunction]
pub fn geometry_col<'py>(
    py: Python<'py>,
    input: AnyRecordBatch,
) -> PyGeoArrowResult<Bound<'py, PyAny>> {
    let schema = input.schema()?;
    let geom_indices = geometry_columns(schema.as_ref());
    let geom_index = if geom_indices.len() == 1 {
        geom_indices[0]
    } else {
        return Err(PyNotImplementedError::new_err(
            "Accessing from multiple geometry columns not yet supported",
        )
        .into());
    };

    let geo_type = GeoArrowType::from_extension_field(schema.field(geom_index))?;

    match input {
        AnyRecordBatch::RecordBatch(batch) => {
            let geo_array = geo_type.wrap_array(batch.as_ref().column(geom_index).as_ref())?;
            Ok(PyGeoArray::new(geo_array).into_geoarrow_py(py)?)
        }
        AnyRecordBatch::Stream(stream) => {
            let reader = stream.into_reader()?;
            let output_geo_type = geo_type.clone();
            let iter = reader
                .into_iter()
                .map(move |batch| geo_type.wrap_array(batch?.column(geom_index).as_ref()));
            let output_reader = Box::new(GeoArrowArrayIterator::new(iter, output_geo_type));

            Ok(PyGeoArrayReader::new(output_reader).into_geoarrow_py(py)?)
        }
    }
}

fn geometry_columns(schema: &Schema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if GeoArrowType::from_extension_field(field).is_ok() {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}
