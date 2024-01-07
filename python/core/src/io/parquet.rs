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
///
/// Returns:
///     Table from GeoParquet file.
#[pyfunction]
pub fn read_parquet(path: String, batch_size: Option<usize>) -> PyGeoArrowResult<GeoTable> {
    let file = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;

    let options = GeoParquetReaderOptions::new(batch_size.unwrap_or(65536), Default::default());
    let table = _read_geoparquet(file, options)?;
    Ok(GeoTable(table))
}
