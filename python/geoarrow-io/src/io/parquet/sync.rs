use std::str::FromStr;
use std::sync::Mutex;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::sync::FileWriter;
use crate::io::input::{AnyFileReader, construct_reader};

use arrow::array::RecordBatchReader;
use geoarrow_geoparquet::{GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder};
use geoarrow_geoparquet::{
    GeoParquetWriter as _GeoParquetWriter, GeoParquetWriterOptions,
    write_geoparquet as _write_geoparquet,
};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PySchema;
use pyo3_arrow::export::Arro3Table;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatch, PyTable};
use pyo3_geoarrow::PyprojCRSTransform;

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=None))]
pub fn read_parquet(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<Arro3Table> {
    let reader = construct_reader(path, store)?;
    match reader {
        #[cfg(feature = "async")]
        AnyFileReader::Async(async_reader) => {
            use crate::runtime::get_runtime;
            use geoarrow_geoparquet::GeoParquetRecordBatchStreamBuilder;
            use parquet::arrow::async_reader::ParquetObjectReader;

            let runtime = get_runtime(py)?;

            let table = runtime.block_on(async move {
                let reader = ParquetObjectReader::new(async_reader.store, async_reader.path);

                let mut geo_options = GeoParquetReaderOptions::default();

                if let Some(batch_size) = batch_size {
                    geo_options = geo_options.with_batch_size(batch_size);
                }

                let (batches, schema) = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
                    reader,
                    ArrowReaderOptions::new().with_page_index(true),
                    geo_options,
                )
                .await?
                .build()?
                .read_table()
                .await?;

                let table = Arro3Table::from(PyTable::try_new(batches, schema).unwrap());
                Ok::<_, PyGeoArrowError>(table)
            })?;
            Ok(table)
        }
        AnyFileReader::Sync(sync_reader) => {
            let mut geo_options = GeoParquetReaderOptions::default();

            if let Some(batch_size) = batch_size {
                geo_options = geo_options.with_batch_size(batch_size);
            }

            let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
                sync_reader,
                ArrowReaderOptions::new().with_page_index(true),
                geo_options,
            )?
            .build()?;
            let schema = reader.schema();

            let batches = reader.collect::<Result<Vec<_>, _>>()?;
            let table = Arro3Table::from(PyTable::try_new(batches, schema).unwrap());
            Ok(table)
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
pub enum GeoParquetEncoding {
    WKB,
    Native,
}

impl<'a> FromPyObject<'a> for GeoParquetEncoding {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
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

impl From<GeoParquetEncoding> for geoarrow_geoparquet::GeoParquetWriterEncoding {
    fn from(value: GeoParquetEncoding) -> Self {
        match value {
            GeoParquetEncoding::WKB => Self::WKB,
            GeoParquetEncoding::Native => Self::Native,
        }
    }
}

pub struct PyWriterVersion(WriterVersion);

impl<'py> FromPyObject<'py> for PyWriterVersion {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Ok(Self(
            WriterVersion::from_str(&s).map_err(|err| PyValueError::new_err(err.to_string()))?,
        ))
    }
}

pub struct PyCompression(Compression);

impl<'py> FromPyObject<'py> for PyCompression {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Ok(Self(
            Compression::from_str(&s).map_err(|err| PyValueError::new_err(err.to_string()))?,
        ))
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        table,
        file,
        *,
        encoding = GeoParquetEncoding::WKB,
        compression = None,
        writer_version = None
    ),
    text_signature = "(table, file, *, encoding = 'WKB', compression = None, writer_version = None)")
]
pub fn write_parquet(
    table: AnyRecordBatch,
    file: FileWriter,
    encoding: GeoParquetEncoding,
    compression: Option<PyCompression>,
    writer_version: Option<PyWriterVersion>,
) -> PyGeoArrowResult<()> {
    let mut props = WriterProperties::builder();

    if let Some(writer_version) = writer_version {
        props = props.set_writer_version(writer_version.0);
    }

    if let Some(compression) = compression {
        props = props.set_compression(compression.0);
    }

    let options = GeoParquetWriterOptions {
        encoding: encoding.into(),
        crs_transform: Some(Box::new(PyprojCRSTransform::new())),
        writer_properties: Some(props.build()),
    };
    _write_geoparquet(table.into_reader()?, file, &options)?;
    Ok(())
}

#[pyclass(module = "geoarrow.rust.io", frozen)]
pub struct ParquetWriter {
    file: Mutex<Option<_GeoParquetWriter<FileWriter>>>,
}

#[pymethods]
impl ParquetWriter {
    #[new]
    pub fn new(py: Python, file: PyObject, schema: PySchema) -> PyGeoArrowResult<Self> {
        let file_writer = file.extract::<FileWriter>(py)?;
        let options = GeoParquetWriterOptions {
            crs_transform: Some(Box::new(PyprojCRSTransform::new())),
            ..Default::default()
        };
        let geoparquet_writer = _GeoParquetWriter::try_new(file_writer, schema.as_ref(), &options)?;
        Ok(Self {
            file: Mutex::new(Some(geoparquet_writer)),
        })
    }

    pub fn __enter__(&self) {}

    pub fn write_batch(&self, batch: PyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.lock().unwrap().as_mut() {
            file.write_batch(batch.as_ref())?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn write_table(&self, table: AnyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.lock().unwrap().as_mut() {
            for batch in table.into_reader()? {
                file.write_batch(&batch?)?;
            }
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn close(&self) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.lock().unwrap().take() {
            file.finish()?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File has already been closed").into())
        }
    }

    pub fn is_closed(&self) -> bool {
        self.file.lock().unwrap().is_none()
    }

    /// Exit the context manager
    #[allow(unused_variables)]
    pub fn __exit__(
        &self,
        r#type: PyObject,
        value: PyObject,
        traceback: PyObject,
    ) -> PyGeoArrowResult<()> {
        self.close()
    }
}
