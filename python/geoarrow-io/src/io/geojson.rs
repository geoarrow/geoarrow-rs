use crate::error::PyGeoArrowResult;
use crate::io::input::sync::{FileReader, FileWriter};
use crate::util::Arro3Table;
use geoarrow::io::geojson::read_geojson as _read_geojson;
use geoarrow::io::geojson::write_geojson as _write_geojson;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatchReader;

#[pyfunction]
#[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_geojson(mut file: FileReader, batch_size: usize) -> PyGeoArrowResult<Arro3Table> {
    let table = _read_geojson(&mut file, Some(batch_size))?;
    Ok(Arro3Table::from_geoarrow(table))
}

#[pyfunction]
pub fn write_geojson(table: PyRecordBatchReader, file: FileWriter) -> PyGeoArrowResult<()> {
    _write_geojson(table.into_reader()?, file)?;
    Ok(())
}
