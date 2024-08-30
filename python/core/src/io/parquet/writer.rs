use crate::error::PyGeoArrowResult;
use crate::io::input::sync::FileWriter;

use geoarrow::io::parquet::{
    write_geoparquet as _write_geoparquet, GeoParquetWriter as _GeoParquetWriter,
    GeoParquetWriterOptions,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatch, PySchema};

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

#[pyfunction]
#[pyo3(
    signature = (table, file, *, encoding = GeoParquetEncoding::WKB),
    text_signature = "(table, file, *, encoding = 'WKB')")
]
pub fn write_parquet(
    table: AnyRecordBatch,
    file: FileWriter,
    encoding: GeoParquetEncoding,
) -> PyGeoArrowResult<()> {
    let options = GeoParquetWriterOptions {
        encoding: encoding.into(),
        ..Default::default()
    };
    _write_geoparquet(table.into_reader()?, file, &options)?;
    Ok(())
}

#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetWriter {
    file: Option<_GeoParquetWriter<FileWriter>>,
}

#[pymethods]
impl ParquetWriter {
    #[new]
    pub fn new(py: Python, file: PyObject, schema: PySchema) -> PyGeoArrowResult<Self> {
        let file_writer = file.extract::<FileWriter>(py)?;
        let geoparquet_writer =
            _GeoParquetWriter::try_new(file_writer, schema.as_ref(), &Default::default())?;
        Ok(Self {
            file: Some(geoparquet_writer),
        })
    }

    pub fn __enter__(&self) {}

    pub fn write_batch(&mut self, batch: PyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.as_mut() {
            file.write_batch(batch.as_ref())?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn write_table(&mut self, table: AnyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.as_mut() {
            for batch in table.into_reader()? {
                file.write_batch(&batch?)?;
            }
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn close(&mut self) -> PyGeoArrowResult<()> {
        if let Some(file) = std::mem::take(&mut self.file) {
            file.finish()?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File has already been closed").into())
        }
    }

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
