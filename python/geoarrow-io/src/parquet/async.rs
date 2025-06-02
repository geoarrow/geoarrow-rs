use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::input::{AnyFileReader, AsyncFileReader, construct_reader};
use crate::parquet::options::{
    PyGeoParquetBboxQuery, PyGeoParquetReadOptions, PyRect, apply_options,
};
#[cfg(feature = "async")]
use crate::runtime::get_runtime;

use futures::TryStreamExt;
use geo_traits::CoordTrait;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::error::GeoArrowError;
use geoparquet::reader::{
    GeoParquetDatasetMetadata, GeoParquetReaderBuilder, GeoParquetReaderMetadata,
    GeoParquetRecordBatchStream,
};
use object_store::ObjectStore;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_arrow::export::{Arro3Array, Arro3Schema, Arro3Table};
use pyo3_arrow::{PyArray, PyTable};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_geoarrow::{PyCoordType, PyCrs};
use pyo3_object_store::AnyObjectStore;

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=None, parse_to_native=true, coord_type=None))]
pub fn read_parquet_async(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: Option<usize>,
    parse_to_native: bool,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(path, store)?;
    match reader {
        AnyFileReader::Async(async_reader) => {
            let fut = future_into_py(py, async move {
                Ok(
                    read_parquet_async_inner(async_reader, batch_size, parse_to_native, coord_type)
                        .await?,
                )
            })?;
            Ok(fut.into())
        }
        AnyFileReader::Sync(_) => {
            Err(PyValueError::new_err("Local file paths not supported in async reader.").into())
        }
    }
}

async fn read_parquet_async_inner(
    async_reader: AsyncFileReader,
    batch_size: Option<usize>,
    parse_to_native: bool,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<Arro3Table> {
    let object_reader = ParquetObjectReader::new(async_reader.store, async_reader.path);
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
        object_reader,
        ArrowReaderOptions::new().with_page_index(true),
    )
    .await?;

    if let Some(batch_size) = batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    let gpq_meta = builder.geoparquet_metadata().ok_or(PyValueError::new_err(
        "Not a GeoParquet file; no `geo` key in Parquet metadata.",
    ))??;
    let geoarrow_schema = builder.geoarrow_schema(
        &gpq_meta,
        parse_to_native,
        coord_type.unwrap_or_default().into(),
    )?;

    let stream = GeoParquetRecordBatchStream::try_new(builder.build()?, geoarrow_schema.clone())?;
    let batches = stream.try_collect().await?;

    let table = Arro3Table::from(PyTable::try_new(batches, geoarrow_schema).unwrap());
    Ok(table)
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "geoarrow.rust.io", frozen)]
pub struct GeoParquetFile {
    path: object_store::path::Path,
    geoparquet_meta: GeoParquetReaderMetadata,
    store: Arc<dyn ObjectStore>,
}

#[pymethods]
impl GeoParquetFile {
    #[classmethod]
    pub(crate) fn open(
        _cls: &Bound<PyType>,
        py: Python,
        path: String,
        store: AnyObjectStore,
    ) -> PyGeoArrowResult<Self> {
        let runtime = get_runtime(py)?;
        let store = store.into_dyn();
        let cloned_store = store.clone();
        let (path, geoparquet_meta) = runtime.block_on(async move {
            let path: object_store::path::Path = path.into();
            let mut reader = ParquetObjectReader::new(cloned_store.clone(), path.clone());
            let arrow_meta = ArrowReaderMetadata::load_async(&mut reader, Default::default())
                .await
                .map_err(|err| GeoArrowError::External(Box::new(err)))?;
            let geoparquet_meta = GeoParquetReaderMetadata::from_arrow_meta(arrow_meta)?;
            Ok::<_, PyGeoArrowError>((path, geoparquet_meta))
        })?;
        Ok(Self {
            path,
            geoparquet_meta,
            store,
        })
    }

    #[classmethod]
    pub(crate) fn open_async<'py>(
        _cls: &Bound<PyType>,
        py: Python<'py>,
        path: String,
        store: AnyObjectStore,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = store.into_dyn();
        let cloned_store = store.clone();
        future_into_py(py, async move {
            let path: object_store::path::Path = path.into();
            let mut reader = ParquetObjectReader::new(cloned_store.clone(), path.clone());
            let arrow_meta = ArrowReaderMetadata::load_async(&mut reader, Default::default())
                .await
                .map_err(|err| GeoArrowError::External(Box::new(err)))
                .map_err(PyGeoArrowError::GeoArrowError)?;
            let geoparquet_meta = GeoParquetReaderMetadata::from_arrow_meta(arrow_meta)
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(Self {
                path,
                geoparquet_meta,
                store,
            })
        })
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.geoparquet_meta.num_rows()
    }

    #[getter]
    fn num_row_groups(&self) -> usize {
        self.geoparquet_meta.num_row_groups()
    }

    fn schema_arrow(
        &self,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<Arro3Schema> {
        let schema = self
            .geoparquet_meta
            .geoarrow_schema(parse_to_native, coord_type.unwrap_or_default().into())?;
        Ok(schema.into())
    }

    #[pyo3(signature = (column_name=None))]
    fn crs(&self, column_name: Option<&str>) -> PyGeoArrowResult<PyCrs> {
        Ok(self.geoparquet_meta.crs(column_name)?.into())
    }

    #[pyo3(signature = (row_group_idx, column_name=None))]
    pub fn row_group_bounds(
        &self,
        row_group_idx: usize,
        column_name: Option<&str>,
    ) -> PyGeoArrowResult<Option<Vec<f64>>> {
        if let Some(bounds) = self
            .geoparquet_meta
            .row_group_bounds(row_group_idx, column_name)?
        {
            Ok(Some(vec![
                bounds.min().x(),
                bounds.min().y(),
                bounds.max().x(),
                bounds.max().y(),
            ]))
        } else {
            Ok(None)
        }
    }

    pub fn row_groups_bounds(&self, column_name: Option<&str>) -> PyGeoArrowResult<Arro3Array> {
        let bounds = self.geoparquet_meta.row_groups_bounds(column_name)?;
        Ok(PyArray::new(
            bounds.to_array_ref(),
            Arc::new(bounds.data_type().to_field("bounds", true)),
        )
        .into())
    }

    #[pyo3(signature = (column_name=None))]
    fn file_bbox<'py>(
        &'py self,
        column_name: Option<&'py str>,
    ) -> PyGeoArrowResult<Option<&'py [f64]>> {
        Ok(self.geoparquet_meta.file_bbox(column_name)?)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, parse_to_native=true, coord_type=None))]
    fn read_async(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<PyRect>,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<PyObject> {
        let options = PyGeoParquetReadOptions {
            batch_size,
            limit,
            offset,
            bbox_query: bbox.map(|bbox| PyGeoParquetBboxQuery {
                bbox,
                column_name: None,
            }),
        };
        let stream = construct_file_stream(
            self.path.clone(),
            self.geoparquet_meta.clone(),
            self.store.clone(),
            options,
            parse_to_native,
            coord_type,
        )?;
        let fut = future_into_py(py, async move {
            let schema = stream.schema().clone();
            let batches = stream
                .try_collect()
                .await
                .map_err(|err| PyGeoArrowError::GeoArrowError(err.into()))?;
            Ok(Arro3Table::from(PyTable::try_new(batches, schema)?))
        })?;
        Ok(fut.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, parse_to_native=true, coord_type=None))]
    fn read(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<PyRect>,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<Arro3Table> {
        let runtime = get_runtime(py)?;
        let options = PyGeoParquetReadOptions {
            batch_size,
            limit,
            offset,
            bbox_query: bbox.map(|bbox| PyGeoParquetBboxQuery {
                bbox,
                column_name: None,
            }),
        };
        let stream = construct_file_stream(
            self.path.clone(),
            self.geoparquet_meta.clone(),
            self.store.clone(),
            options,
            parse_to_native,
            coord_type,
        )?;
        runtime.block_on(async move {
            let schema = stream.schema().clone();
            let batches = stream
                .try_collect()
                .await
                .map_err(|err| PyGeoArrowError::GeoArrowError(err.into()))?;
            Ok(Arro3Table::from(PyTable::try_new(batches, schema)?))
        })
    }
}

fn construct_file_stream(
    path: object_store::path::Path,
    geoparquet_meta: GeoParquetReaderMetadata,
    store: Arc<dyn ObjectStore>,
    options: PyGeoParquetReadOptions,
    parse_to_native: bool,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<GeoParquetRecordBatchStream<ParquetObjectReader>> {
    let object_reader = ParquetObjectReader::new(store, path);
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        object_reader,
        geoparquet_meta.arrow_metadata().clone(),
    );

    let geoarrow_schema = builder.geoarrow_schema(
        geoparquet_meta.geo_metadata(),
        parse_to_native,
        coord_type.unwrap_or_default().into(),
    )?;
    let builder = apply_options(builder, geoparquet_meta.geo_metadata(), options)?;

    let stream = GeoParquetRecordBatchStream::try_new(builder.build()?, geoarrow_schema.clone())?;
    Ok(stream)
}

/// Create a reader per path with the given ObjectStore instance.
// TODO: deduplicate with JS binding
async fn fetch_arrow_metadata_objects(
    paths: Vec<String>,
    store: Arc<dyn ObjectStore>,
) -> Result<HashMap<String, ArrowReaderMetadata>, GeoArrowError> {
    let paths: Vec<object_store::path::Path> = paths.into_iter().map(|path| path.into()).collect();
    let mut readers = paths
        .iter()
        .map(|path| ParquetObjectReader::new(store.clone(), path.clone()))
        .collect::<Vec<_>>();
    let parquet_meta_futures = readers
        .iter_mut()
        .map(|reader| ArrowReaderMetadata::load_async(reader, Default::default()));
    let parquet_metas = futures::future::join_all(parquet_meta_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, parquet::errors::ParquetError>>()
        .map_err(|err| GeoArrowError::External(Box::new(err)))?;

    let mut hashmap: HashMap<String, ArrowReaderMetadata> = HashMap::new();
    for (path, arrow_meta) in paths.iter().zip(parquet_metas) {
        hashmap.insert(path.to_string(), arrow_meta);
    }

    Ok(hashmap)
}

/// Encapsulates details of reading a complete Parquet dataset possibly consisting of multiple
/// files and partitions in subdirectories.
#[pyclass(module = "geoarrow.rust.io", frozen)]
pub struct GeoParquetDataset {
    meta: GeoParquetDatasetMetadata,
    store: Arc<dyn ObjectStore>,
}

impl GeoParquetDataset {
    fn to_readers(
        &self,
        options: PyGeoParquetReadOptions,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<Vec<GeoParquetRecordBatchStream<ParquetObjectReader>>> {
        self.meta
            .files()
            .iter()
            .map(|(path, meta)| {
                // TODO: don't re-parse GeoParquet metadata
                let geoparquet_meta = GeoParquetReaderMetadata::from_arrow_meta(meta.clone())?;
                construct_file_stream(
                    path.clone().into(),
                    geoparquet_meta,
                    self.store.clone(),
                    options.clone(),
                    parse_to_native,
                    coord_type,
                )
            })
            .collect()
    }

    async fn read_inner(
        readers: Vec<GeoParquetRecordBatchStream<ParquetObjectReader>>,
    ) -> PyGeoArrowResult<Arro3Table> {
        let output_schema = readers[0].schema();

        let request_futures = readers.into_iter().map(|reader| async move {
            reader
                .try_collect::<Vec<_>>()
                .await
                .map_err(|err| PyGeoArrowError::GeoArrowError(err.into()))
        });
        let record_batches = futures::future::join_all(request_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let record_batches = record_batches.into_iter().flatten().collect::<Vec<_>>();

        Ok(Arro3Table::from(PyTable::try_new(
            record_batches,
            output_schema,
        )?))
    }
}

#[pymethods]
impl GeoParquetDataset {
    #[new]
    pub fn new(py: Python, paths: Vec<String>, store: AnyObjectStore) -> PyGeoArrowResult<Self> {
        let runtime = get_runtime(py)?;
        let store = store.into_dyn();
        let cloned_store = store.clone();

        let meta = runtime.block_on(async move {
            let meta = fetch_arrow_metadata_objects(paths, store.clone()).await?;
            Ok::<_, PyGeoArrowError>(meta)
        })?;

        Ok(Self {
            meta: GeoParquetDatasetMetadata::from_files(meta)?,
            store: cloned_store,
        })
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.meta.num_rows()
    }

    #[getter]
    fn num_row_groups(&self) -> usize {
        self.meta.num_row_groups()
    }

    fn schema_arrow(
        &self,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<Arro3Schema> {
        let schema = self
            .meta
            .geoarrow_schema(parse_to_native, coord_type.unwrap_or_default().into())?;
        Ok(schema.into())
    }

    #[pyo3(signature = (column_name=None))]
    fn crs(&self, column_name: Option<&str>) -> PyGeoArrowResult<PyCrs> {
        Ok(self.meta.crs(column_name)?.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, parse_to_native=true, coord_type=None))]
    fn read_async<'py>(
        &self,
        py: Python<'py>,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<PyRect>,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<Bound<'py, PyAny>> {
        let options = PyGeoParquetReadOptions {
            batch_size,
            limit,
            offset,
            bbox_query: bbox.map(|bbox| PyGeoParquetBboxQuery {
                bbox,
                column_name: None,
            }),
        };
        let readers = self.to_readers(options, parse_to_native, coord_type)?;
        let fut = future_into_py(py, async move { Ok(Self::read_inner(readers).await?) })?;
        Ok(fut)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, parse_to_native=true, coord_type=None))]
    fn read(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<PyRect>,
        parse_to_native: bool,
        coord_type: Option<PyCoordType>,
    ) -> PyGeoArrowResult<Arro3Table> {
        let runtime = get_runtime(py)?;
        let options = PyGeoParquetReadOptions {
            batch_size,
            limit,
            offset,
            bbox_query: bbox.map(|bbox| PyGeoParquetBboxQuery {
                bbox,
                column_name: None,
            }),
        };
        let readers = self.to_readers(options, parse_to_native, coord_type)?;
        runtime.block_on(Self::read_inner(readers))
    }
}
