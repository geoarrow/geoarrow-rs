use std::str::FromStr;
use std::sync::Mutex;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::input::sync::FileWriter;
use crate::input::{AnyFileReader, construct_reader};

use arrow::datatypes::SchemaRef;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PySchema;
use pyo3_arrow::export::Arro3Table;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatch, PyTable};
use pyo3_geoarrow::{PyCoordType, PyprojCRSTransform};

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=None, parse_to_native=true, coord_type=None))]
pub fn read_parquet(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: Option<usize>,
    parse_to_native: bool,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<Arro3Table> {
    let reader = construct_reader(path, store)?;
    match reader {
        #[cfg(feature = "async")]
        AnyFileReader::Async(async_reader) => {
            use futures::TryStreamExt;
            use geoparquet::reader::GeoParquetRecordBatchStream;
            use parquet::arrow::async_reader::{
                ParquetObjectReader, ParquetRecordBatchStreamBuilder,
            };

            use crate::runtime::get_runtime;

            let runtime = get_runtime(py)?;

            py.detach(|| {
                let table = runtime.block_on(async move {
                    let object_reader =
                        ParquetObjectReader::new(async_reader.store, async_reader.path);
                    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
                        object_reader,
                        ArrowReaderOptions::new().with_page_index(true),
                    )
                    .await?;

                    if let Some(batch_size) = batch_size {
                        builder = builder.with_batch_size(batch_size);
                    }

                    let gpq_meta = builder.geoparquet_metadata().ok_or(
                        PyValueError::new_err("Not a GeoParquet file; no GeoParquet metadata."),
                    )??;
                    let geoarrow_schema = builder.geoarrow_schema(
                        &gpq_meta,
                        parse_to_native,
                        coord_type.unwrap_or_default().into(),
                    )?;

                    let stream = GeoParquetRecordBatchStream::try_new(
                        builder.build()?,
                        geoarrow_schema.clone(),
                    )?;
                    let batches = stream.try_collect().await?;

                    let table =
                        Arro3Table::from(PyTable::try_new(batches, geoarrow_schema).unwrap());
                    Ok::<_, PyGeoArrowError>(table)
                })?;
                Ok(table)
            })
        }
        AnyFileReader::Sync(sync_reader) => {
            let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
                sync_reader,
                ArrowReaderOptions::new().with_page_index(true),
            )?;

            if let Some(batch_size) = batch_size {
                builder = builder.with_batch_size(batch_size);
            }

            let gpq_meta = builder.geoparquet_metadata().ok_or(PyValueError::new_err(
                "Not a GeoParquet file; no GeoParquet metadata.",
            ))??;
            let geoarrow_schema = builder.geoarrow_schema(
                &gpq_meta,
                parse_to_native,
                coord_type.unwrap_or_default().into(),
            )?;

            let reader =
                GeoParquetRecordBatchReader::try_new(builder.build()?, geoarrow_schema.clone())?;
            let batches = reader.collect::<Result<Vec<_>, _>>()?;

            let table = Arro3Table::from(PyTable::try_new(batches, geoarrow_schema).unwrap());
            Ok(table)
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
pub enum GeoParquetEncoding {
    WKB,
    GeoArrow,
}

impl<'a, 'py> FromPyObject<'a, 'py> for GeoParquetEncoding {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "wkb" => Ok(Self::WKB),
            "geoarrow" => Ok(Self::GeoArrow),
            _ => Err(PyValueError::new_err(
                "Unexpected encoding. Should be one of 'WKB' or 'geoarrow'.",
            )),
        }
    }
}

impl From<GeoParquetEncoding> for geoparquet::writer::GeoParquetWriterEncoding {
    fn from(value: GeoParquetEncoding) -> Self {
        match value {
            GeoParquetEncoding::WKB => Self::WKB,
            GeoParquetEncoding::GeoArrow => Self::GeoArrow,
        }
    }
}

pub struct PyWriterVersion(WriterVersion);

impl<'a, 'py> FromPyObject<'a, 'py> for PyWriterVersion {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Ok(Self(
            WriterVersion::from_str(&s).map_err(|err| PyValueError::new_err(err.to_string()))?,
        ))
    }
}

pub struct PyCompression(Compression);

impl<'a, 'py> FromPyObject<'a, 'py> for PyCompression {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
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
        compression = PyCompression(Compression::ZSTD(Default::default())),
        writer_version = PyWriterVersion(WriterVersion::PARQUET_2_0),
        generate_covering = false,
    ),
    text_signature = "(table, file, *, encoding = 'WKB', compression = 'zstd(1)', writer_version = 'parquet_2_0', generate_covering = False)")
]
pub fn write_parquet(
    table: AnyRecordBatch,
    file: FileWriter,
    encoding: GeoParquetEncoding,
    compression: Option<PyCompression>,
    writer_version: Option<PyWriterVersion>,
    generate_covering: bool,
) -> PyGeoArrowResult<()> {
    let writer = PyGeoParquetWriter::new(
        file,
        table.schema()?,
        encoding,
        compression,
        writer_version,
        generate_covering,
    )?;
    let reader = table.into_reader()?;
    for batch in reader {
        writer.write_batch(PyRecordBatch::new(batch?))?;
    }
    writer.close()?;
    Ok(())
}

#[pyclass(module = "geoarrow.rust.io", name = "GeoParquetWriter", frozen)]
pub struct PyGeoParquetWriter {
    writer: Mutex<Option<ArrowWriter<FileWriter>>>,
    gpq_encoder: Mutex<Option<GeoParquetRecordBatchEncoder>>,
}

impl PyGeoParquetWriter {
    fn new(
        file: FileWriter,
        schema: SchemaRef,
        encoding: GeoParquetEncoding,
        compression: Option<PyCompression>,
        writer_version: Option<PyWriterVersion>,
        generate_covering: bool,
    ) -> PyGeoArrowResult<Self> {
        let mut writer_properties = WriterProperties::builder();

        if let Some(writer_version) = writer_version {
            writer_properties = writer_properties.set_writer_version(writer_version.0);
        }

        if let Some(compression) = compression {
            writer_properties = writer_properties.set_compression(compression.0);
        }

        let options_builder = GeoParquetWriterOptionsBuilder::default()
            .set_crs_transform(Box::new(PyprojCRSTransform::new()))
            .set_encoding(encoding.into())
            .set_generate_covering(generate_covering);

        let gpq_encoder =
            GeoParquetRecordBatchEncoder::try_new(schema.as_ref(), &options_builder.build())?;
        let parquet_writer = ArrowWriter::try_new(
            file,
            gpq_encoder.target_schema(),
            Some(writer_properties.build()),
        )?;
        Ok(Self {
            writer: Mutex::new(Some(parquet_writer)),
            gpq_encoder: Mutex::new(Some(gpq_encoder)),
        })
    }
}

#[pymethods]
impl PyGeoParquetWriter {
    #[pyo3(
        signature = (
            file,
            schema,
            *,
            encoding = GeoParquetEncoding::WKB,
            compression = PyCompression(Compression::ZSTD(Default::default())),
            writer_version = PyWriterVersion(WriterVersion::PARQUET_2_0),
            generate_covering = false,
        ),
        text_signature = "(file, schema, *, encoding='WKB', compression='zstd(1)', writer_version='parquet_2_0', generate_covering=False)")
    ]
    #[new]
    pub fn py_new(
        py: Python,
        file: Py<PyAny>,
        schema: PySchema,
        encoding: GeoParquetEncoding,
        compression: Option<PyCompression>,
        writer_version: Option<PyWriterVersion>,
        generate_covering: bool,
    ) -> PyGeoArrowResult<Self> {
        Self::new(
            file.extract(py)?,
            schema.into_inner(),
            encoding,
            compression,
            writer_version,
            generate_covering,
        )
    }

    pub fn __enter__(slf: PyRef<Self>) -> PyResult<PyRef<Self>> {
        if slf.is_closed() {
            Err(PyValueError::new_err("File is already closed."))
        } else {
            Ok(slf)
        }
    }

    pub fn write_batch(&self, batch: PyRecordBatch) -> PyGeoArrowResult<()> {
        if let (Some(writer), Some(gpq_encoder)) = (
            self.writer.lock().unwrap().as_mut(),
            self.gpq_encoder.lock().unwrap().as_mut(),
        ) {
            let encoded_batch = gpq_encoder.encode_record_batch(batch.as_ref())?;
            writer.write(&encoded_batch)?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn write_table(&self, table: AnyRecordBatch) -> PyGeoArrowResult<()> {
        if let (Some(writer), Some(gpq_encoder)) = (
            self.writer.lock().unwrap().as_mut(),
            self.gpq_encoder.lock().unwrap().as_mut(),
        ) {
            for batch in table.into_reader()? {
                let encoded_batch = gpq_encoder.encode_record_batch(&batch?)?;
                writer.write(&encoded_batch)?;
            }
            Ok(())
        } else {
            Err(PyValueError::new_err("File is already closed.").into())
        }
    }

    pub fn close(&self) -> PyGeoArrowResult<()> {
        if let (Some(mut writer), Some(gpq_encoder)) = (
            self.writer.lock().unwrap().take(),
            self.gpq_encoder.lock().unwrap().take(),
        ) {
            let kv_metadata = gpq_encoder.into_keyvalue()?;
            writer.append_key_value_metadata(kv_metadata);
            writer.finish()?;
            Ok(())
        } else {
            Err(PyValueError::new_err("File has already been closed").into())
        }
    }

    pub fn is_closed(&self) -> bool {
        self.writer.lock().unwrap().is_none()
    }

    /// Exit the context manager
    #[allow(unused_variables)]
    pub fn __exit__(
        &self,
        r#type: Py<PyAny>,
        value: Py<PyAny>,
        traceback: Py<PyAny>,
    ) -> PyGeoArrowResult<()> {
        self.close()
    }
}
