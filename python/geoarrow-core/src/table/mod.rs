mod geo_interface;

use crate::ffi::to_python::{chunked_native_array_to_pyobject, native_array_to_pyobject};
use crate::interop::util::pytable_to_table;
use geoarrow::array::NativeArrayDyn;
use geoarrow::schema::GeoSchemaExt;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn geometry_col(py: Python, input: AnyRecordBatch) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyRecordBatch::RecordBatch(rb) => {
            let batch = rb.into_inner();
            let schema = batch.schema();

            let geom_indices = schema.as_ref().geometry_columns();
            let index = if geom_indices.len() == 1 {
                geom_indices[0]
            } else {
                return Err(PyNotImplementedError::new_err(
                    "Accessing from multiple geometry columns not yet supported",
                )
                .into());
            };

            let field = schema.field(index);
            let array = batch.column(index).as_ref();
            let geo_arr = NativeArrayDyn::from_arrow_array(array, field)?.into_inner();
            native_array_to_pyobject(py, geo_arr)
        }
        AnyRecordBatch::Stream(stream) => {
            let table = stream.into_table()?;
            let table = pytable_to_table(table)?;
            let chunked_geom_arr = table.geometry_column(None)?;
            chunked_native_array_to_pyobject(py, chunked_geom_arr)
        }
    }
}
