use std::fs::File;
use std::io::{BufReader, BufWriter};

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::csv::read_csv as _read_csv;
use geoarrow::io::csv::write_csv as _write_csv;
use geoarrow::io::csv::CSVReaderOptions;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a CSV file from a path on disk into a GeoTable.
///
/// Args:
///     path: the path to the file
///     geometry_column_name: the name of the geometry column within the CSV.
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from CSV file.
#[pyfunction]
pub fn read_csv(
    path: String,
    geometry_column_name: &str,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let options = CSVReaderOptions::new(Default::default(), batch_size.unwrap_or(65536));
    let table = _read_csv(&mut reader, geometry_column_name, options)?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a CSV file on disk.
///
/// Args:
///     table: the table to write.
///     path: the path to the file.
///
/// Returns:
///     None
#[pyfunction]
pub fn write_csv(table: &PyAny, path: String) -> PyGeoArrowResult<()> {
    let mut table: GeoTable = FromPyObject::extract(table)?;
    let f = File::create(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let writer = BufWriter::new(f);
    _write_csv(&mut table.0, writer)?;
    Ok(())
}
