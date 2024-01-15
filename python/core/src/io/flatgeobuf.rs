use std::fs::File;
use std::io::{BufReader, BufWriter};

use crate::error::PyGeoArrowResult;
use crate::io::file::PyFileLikeObject;
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyType};

pub enum BinaryFileInput {
    String(String),
    FileLike(PyFileLikeObject),
}

impl<'a> FromPyObject<'a> for BinaryFileInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if let Ok(string_ref) = ob.downcast::<PyString>() {
            return Ok(Self::String(string_ref.to_string_lossy().to_string()));
        }

        Python::with_gil(|py| {
            let module = PyModule::import(py, "pathlib")?;
            let path = module.getattr(intern!(py, "Path"))?;
            let path_type = path.extract::<&PyType>()?;
            if ob.is_instance(path_type)? {
                return Ok(Self::String(ob.to_string()));
            }

            match PyFileLikeObject::with_requirements(ob.into(), true, false, true, false) {
                Ok(f) => Ok(Self::FileLike(f)),
                Err(e) => Err(e),
            }
        })
    }
}

/// Read a FlatGeobuf file from a path on disk into a GeoTable.
///
/// Args:
///     file: the path to the file
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
pub fn read_flatgeobuf(
    file: BinaryFileInput,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<GeoTable> {
    let table = match file {
        BinaryFileInput::String(s) => {
            let mut reader = BufReader::new(
                File::open(s).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
            );
            _read_flatgeobuf(&mut reader, Default::default(), batch_size)?
        }
        BinaryFileInput::FileLike(f) => {
            let mut reader = BufReader::new(f);
            _read_flatgeobuf(&mut reader, Default::default(), batch_size)?
        }
    };
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
