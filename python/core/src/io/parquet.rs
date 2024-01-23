use std::fs::File;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::parquet::read_geoparquet as _read_geoparquet;
use geoarrow::io::parquet::GeoParquetReaderOptions;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a GeoParquet file from a path on disk into a GeoTable.
///
/// Args:
///     path: the path to the file
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from GeoParquet file.
#[pyfunction]
#[pyo3(signature = (path, *, batch_size=65536))]
pub fn read_parquet(path: String, batch_size: usize) -> PyGeoArrowResult<GeoTable> {
    let file = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;

    let options = GeoParquetReaderOptions::new(batch_size, Default::default());
    let table = _read_geoparquet(file, options)?;
    Ok(GeoTable(table))
}
