use crate::error::PyGeoArrowResult;
use crate::interop::util::table_to_pytable;
use crate::io::input::sync::{FileReader, FileWriter};
use geoarrow::io::geojson_lines::read_geojson_lines as _read_geojson_lines;
use geoarrow::io::geojson_lines::write_geojson_lines as _write_geojson_lines;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyRecordBatch;

#[pyfunction]
#[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_geojson_lines(
    py: Python,
    mut file: FileReader,
    batch_size: usize,
) -> PyGeoArrowResult<PyObject> {
    let table = _read_geojson_lines(&mut file, Some(batch_size))?;
    Ok(table_to_pytable(table).to_arro3(py)?)
}

#[pyfunction]
pub fn write_geojson_lines(table: AnyRecordBatch, file: FileWriter) -> PyGeoArrowResult<()> {
    _write_geojson_lines(table.into_reader()?, file)?;
    Ok(())
}
