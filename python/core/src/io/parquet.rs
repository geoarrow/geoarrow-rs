use std::fs::File;
use std::io::BufWriter;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::parquet::read_geoparquet as _read_geoparquet;
use geoarrow::io::parquet::write_geoparquet as _write_geoparquet;
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

/// Write a GeoTable to a GeoParquet file on disk.
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
pub fn write_parquet(mut table: GeoTable, file: String) -> PyGeoArrowResult<()> {
    let writer = BufWriter::new(
        File::create(file).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
    );

    _write_geoparquet(&mut table.0, writer, None)?;
    Ok(())
}
