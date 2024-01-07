use std::fs::File;
use std::io::{BufReader, BufWriter};

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a FlatGeobuf file from a path on disk into a GeoTable.
///
/// Args:
///     path: the path to the file
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
pub fn read_flatgeobuf(path: String, batch_size: Option<usize>) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let table = _read_flatgeobuf(&mut reader, Default::default(), batch_size)?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a FlatGeobuf file on disk.
///
/// Args:
///     table: the table to write.
///     path: the path to the file.
///
/// Returns:
///     None
#[pyfunction]
#[pyo3(signature = (table, path, *, write_index=true))]
pub fn write_flatgeobuf(table: &PyAny, path: String, write_index: bool) -> PyGeoArrowResult<()> {
    let mut table: GeoTable = FromPyObject::extract(table)?;
    let f = File::create(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let writer = BufWriter::new(f);
    let options = FgbWriterOptions {
        write_index,
        ..Default::default()
    };
    _write_flatgeobuf(&mut table.0, writer, "", options)?;
    Ok(())
}
