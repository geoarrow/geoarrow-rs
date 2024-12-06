use std::fs::File;
use std::sync::Mutex;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::sync::{FileReader, FileWriter};
use crate::io::input::{construct_reader, AnyFileReader};
use crate::util::table_to_pytable;

use geoarrow::io::parquet::{GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use pyo3_arrow::PySchema;

use geoarrow::io::parquet::{
    write_geoparquet as _write_geoparquet, GeoParquetWriter as _GeoParquetWriter,
    GeoParquetWriterOptions,
};
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_geoarrow::PyprojCRSTransform;

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=None))]
pub fn read_parquet(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(path, store)?;
    match reader {
        #[cfg(feature = "async")]
        AnyFileReader::Async(async_reader) => {
            use crate::runtime::get_runtime;
            use geoarrow::io::parquet::GeoParquetRecordBatchStreamBuilder;
            use object_store::ObjectStore;
            use parquet::arrow::async_reader::ParquetObjectReader;

            let runtime = get_runtime(py)?;

            let table = runtime.block_on(async move {
                let object_meta = async_reader
                    .store
                    .head(&async_reader.path)
                    .await
                    .map_err(PyGeoArrowError::ObjectStoreError)?;
                let reader = ParquetObjectReader::new(async_reader.store, object_meta);

                let mut geo_options = GeoParquetReaderOptions::default();

                if let Some(batch_size) = batch_size {
                    geo_options = geo_options.with_batch_size(batch_size);
                }

                let table = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
                    reader,
                    ArrowReaderOptions::new().with_page_index(true),
                    geo_options,
                )
                .await?
                .build()?
                .read_table()
                .await?;

                Ok::<_, PyGeoArrowError>(table_to_pytable(table).to_arro3(py)?)
            })?;
            Ok(table)
        }
        AnyFileReader::Sync(sync_reader) => match sync_reader {
            FileReader::File(path, _) => {
                let file = File::open(path)
                    .map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;

                let mut geo_options = GeoParquetReaderOptions::default();

                if let Some(batch_size) = batch_size {
                    geo_options = geo_options.with_batch_size(batch_size);
                }

                let table = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
                    file,
                    ArrowReaderOptions::new().with_page_index(true),
                    geo_options,
                )?
                .build()?
                .read_table()?;
                Ok(table_to_pytable(table).to_arro3(py)?)
            }
            _ => Err(PyValueError::new_err("File objects not supported in Parquet reader.").into()),
        },
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
        crs_transform: Some(Box::new(PyprojCRSTransform::new())),
        ..Default::default()
    };
    _write_geoparquet(table.into_reader()?, file, &options)?;
    Ok(())
}

#[pyclass(module = "geoarrow.rust.io._io")]
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

    pub fn write_batch(&mut self, batch: PyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.lock().unwrap().as_mut() {
            file.write_batch(batch.as_ref())?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn write_table(&mut self, table: AnyRecordBatch) -> PyGeoArrowResult<()> {
        if let Some(file) = self.file.lock().unwrap().as_mut() {
            for batch in table.into_reader()? {
                file.write_batch(&batch?)?;
            }
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn close(&mut self) -> PyGeoArrowResult<()> {
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
        &mut self,
        r#type: PyObject,
        value: PyObject,
        traceback: PyObject,
    ) -> PyGeoArrowResult<()> {
        self.close()
    }
}
