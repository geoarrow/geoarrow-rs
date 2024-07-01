use std::fs::File;
use std::io::BufWriter;

use crate::error::PyGeoArrowResult;
use crate::interop::util::pytable_to_table;
use crate::io::input::sync::BinaryFileWriter;

use geoarrow::io::parquet::{
    write_geoparquet as _write_geoparquet, GeoParquetWriter as _GeoParquetWriter,
    GeoParquetWriterOptions,
};
use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::{PyRecordBatch, PySchema, PyTable};

pub enum GeoParquetEncoding {
    WKB,
    Native,
}

impl<'a> FromPyObject<'a> for GeoParquetEncoding {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "wkb" => Ok(Self::WKB),
            "native" => Ok(Self::Native),
            _ => Err(PyValueError::new_err(
                "Unexpected encoding. Should be one of 'WKB' or 'native'.",
            )),
        }
    }
}

impl From<GeoParquetEncoding> for geoarrow::io::parquet::GeoParquetWriterEncoding {
    fn from(value: GeoParquetEncoding) -> Self {
        match value {
            GeoParquetEncoding::WKB => Self::WKB,
            GeoParquetEncoding::Native => Self::Native,
        }
    }
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
#[pyo3(
    signature = (table, file, *, encoding = GeoParquetEncoding::WKB),
    text_signature = "(table, file, *, encoding = 'WKB')")
]
pub fn write_parquet(
    table: PyTable,
    file: String,
    encoding: GeoParquetEncoding,
) -> PyGeoArrowResult<()> {
    let writer = BufWriter::new(
        File::create(file).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
    );
    let options = GeoParquetWriterOptions {
        encoding: encoding.into(),
        ..Default::default()
    };
    let mut table = pytable_to_table(table)?;
    _write_geoparquet(&mut table, writer, &options)?;
    Ok(())
}

/// Writer interface for a single Parquet file.
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetWriter {
    file: Option<_GeoParquetWriter<BinaryFileWriter>>,
}

#[pymethods]
impl ParquetWriter {
    #[new]
    pub fn new(py: Python, file: PyObject, schema: PySchema) -> PyGeoArrowResult<Self> {
        let file_writer = file.extract::<BinaryFileWriter>(py)?;
        let geoparquet_writer =
            _GeoParquetWriter::try_new(file_writer, schema.as_ref(), &Default::default())?;
        Ok(Self {
            file: Some(geoparquet_writer),
        })
    }

    /// Enter the context manager
    pub fn __enter__(&self) {}

    /// Write a single record batch to the Parquet file
    pub fn write_batch(&mut self, batch: PyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.as_mut() {
            file.write_batch(batch.as_ref())?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    /// Write a table or stream of batches to the Parquet file
    pub fn write_table(&mut self, table: PyTable) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.as_mut() {
            for batch in table.batches() {
                file.write_batch(batch)?;
            }
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    /// Close this file.
    ///
    /// The recommended use of this class is as a context manager, which will close the file
    /// automatically.
    pub fn close(&mut self) -> PyGeoArrowResult<()> {
        if let Some(file) = std::mem::take(&mut self.file) {
            file.finish()?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File has already been closed").into())
        }
    }

    /// Returns `True` if the file has already been closed.
    pub fn is_closed(&self) -> bool {
        self.file.is_none()
    }

    /// Exit the context manager
    #[allow(unused_variables)]
    pub fn __exit__(
        &mut self,
        r#type: PyObject,
        value: PyObject,
        traceback: PyObject,
    ) -> PyGeoArrowResult<()> {
        self.close()
    }
}
