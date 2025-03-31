use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::{construct_reader, AnyFileReader, AsyncFileReader};
use crate::io::parquet::options::{create_options, PyGeoParquetBboxCovering};
#[cfg(feature = "async")]
use crate::runtime::get_runtime;
use crate::util::to_arro3_table;

use arrow::datatypes::SchemaRef;
use geo_traits::CoordTrait;
use geoarrow::error::GeoArrowError;
use geoarrow::io::parquet::metadata::GeoParquetBboxCovering;
use geoarrow::io::parquet::{
    GeoParquetDatasetMetadata, GeoParquetReaderMetadata, GeoParquetReaderOptions,
    GeoParquetRecordBatchStream, GeoParquetRecordBatchStreamBuilder,
};
use geoarrow::table::Table;
use geoarrow::ArrayBase;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::export::{Arro3Schema, Arro3Table};
use pyo3_arrow::PyArray;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_geoarrow::CRS;
use pyo3_object_store::AnyObjectStore;

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=None))]
pub fn read_parquet_async(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(path, store)?;
    match reader {
        AnyFileReader::Async(async_reader) => {
            let fut = future_into_py(py, async move {
                Ok(read_parquet_async_inner(async_reader, batch_size).await?)
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
) -> PyGeoArrowResult<Arro3Table> {
    let object_meta = async_reader.store.head(&async_reader.path).await?;
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

    Ok(to_arro3_table(table))
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "geoarrow.rust.io", frozen)]
pub struct ParquetFile {
    object_meta: object_store::ObjectMeta,
    geoparquet_meta: GeoParquetReaderMetadata,
    store: Arc<dyn ObjectStore>,
}

#[pymethods]
impl ParquetFile {
    // TODO: change this to aenter
    #[new]
    pub fn new(py: Python, path: String, store: AnyObjectStore) -> PyGeoArrowResult<Self> {
        let runtime = get_runtime(py)?;
        let store = store.into_dyn();
        let cloned_store = store.clone();
        let store_ref = store.as_ref();
        let (object_meta, geoparquet_meta) = runtime.block_on(async move {
            let object_meta = store_ref
                .head(&path.into())
                .await
                .map_err(GeoArrowError::ObjectStoreError)?;
            let mut reader = ParquetObjectReader::new(cloned_store.clone(), object_meta.clone());
            let arrow_meta = ArrowReaderMetadata::load_async(&mut reader, Default::default())
                .await
                .map_err(GeoArrowError::ParquetError)?;
            let geoparquet_meta = GeoParquetReaderMetadata::new(arrow_meta);
            Ok::<_, PyGeoArrowError>((object_meta, geoparquet_meta))
        })?;
        Ok(Self {
            object_meta,
            geoparquet_meta,
            store,
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

    #[getter]
    fn schema_arrow(&self) -> PyGeoArrowResult<Arro3Schema> {
        let schema = self.geoparquet_meta.resolved_schema(Default::default())?;
        Ok(schema.into())
    }

    #[pyo3(signature = (column_name=None))]
    fn crs(&self, py: Python, column_name: Option<&str>) -> PyGeoArrowResult<PyObject> {
        if let Some(crs) = self.geoparquet_meta.crs(column_name)? {
            Ok(CRS::from_projjson(crs.clone())
                .to_pyproj(py)
                .map_err(PyErr::from)?)
        } else {
            Ok(py.None())
        }
    }

    #[pyo3(signature = (row_group_idx, bbox_paths=None))]
    pub fn row_group_bounds(
        &self,
        row_group_idx: usize,
        bbox_paths: Option<PyGeoParquetBboxCovering>,
    ) -> PyGeoArrowResult<Option<Vec<f64>>> {
        let paths: Option<GeoParquetBboxCovering> = bbox_paths.map(|x| x.into());

        if let Some(bounds) = self
            .geoparquet_meta
            .row_group_bounds(row_group_idx, paths.as_ref())?
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

    #[pyo3(signature = (bbox_paths=None))]
    pub fn row_groups_bounds(
        &self,
        py: Python,
        bbox_paths: Option<PyGeoParquetBboxCovering>,
    ) -> PyGeoArrowResult<PyObject> {
        let paths: Option<GeoParquetBboxCovering> = bbox_paths.map(|x| x.into());
        let bounds = self.geoparquet_meta.row_groups_bounds(paths.as_ref())?;
        Ok(
            PyArray::new(bounds.to_array_ref(), bounds.extension_field())
                .to_arro3(py)?
                .unbind(),
        )
    }

    #[pyo3(signature = (column_name=None))]
    fn file_bbox(&self, column_name: Option<&str>) -> PyGeoArrowResult<Option<&[f64]>> {
        Ok(self.geoparquet_meta.file_bbox(column_name)?)
    }

    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read_async(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<PyGeoParquetBboxCovering>,
    ) -> PyGeoArrowResult<PyObject> {
        let reader = ParquetObjectReader::new(self.store.clone(), self.object_meta.clone());
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let stream = GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
            reader,
            self.geoparquet_meta.clone(),
            options,
        )
        .build()?;
        let fut = future_into_py(py, async move {
            let table = stream
                .read_table()
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(to_arro3_table(table))
        })?;
        Ok(fut.into())
    }

    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<PyGeoParquetBboxCovering>,
    ) -> PyGeoArrowResult<Arro3Table> {
        let runtime = get_runtime(py)?;
        let reader = ParquetObjectReader::new(self.store.clone(), self.object_meta.clone());
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let stream = GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
            reader,
            self.geoparquet_meta.clone(),
            options,
        )
        .build()?;
        runtime.block_on(async move {
            let table = stream
                .read_table()
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(to_arro3_table(table))
        })
    }
}

// Remove once we ensure that below method is working
//
// /// Create a reader per path with the given ObjectStore instance.
// async fn fetch_geoparquet_metas(
//     paths: Vec<String>,
//     store: Arc<dyn ObjectStore>,
// ) -> PyGeoArrowResult<
//     HashMap<object_store::path::Path, Vec<(ParquetObjectReader, GeoParquetReaderMetadata)>>,
// > {
//     let paths: Vec<object_store::path::Path> = paths.into_iter().map(|path| path.into()).collect();
//     let object_meta_futures = paths.iter().map(|path| store.head(path));
//     let object_metas = futures::future::join_all(object_meta_futures)
//         .await
//         .into_iter()
//         .collect::<Result<Vec<_>, object_store::Error>>()
//         .map_err(GeoArrowError::ObjectStoreError)?;
//     let mut readers = object_metas
//         .into_iter()
//         .map(|meta| ParquetObjectReader::new(store.clone(), meta))
//         .collect::<Vec<_>>();
//     let parquet_meta_futures = readers
//         .iter_mut()
//         .map(|reader| ArrowReaderMetadata::load_async(reader, Default::default()));
//     let parquet_metas = futures::future::join_all(parquet_meta_futures)
//         .await
//         .into_iter()
//         .collect::<Result<Vec<_>, parquet::errors::ParquetError>>()
//         .map_err(GeoArrowError::ParquetError)?;

//     let mut hashmap: HashMap<
//         object_store::path::Path,
//         Vec<(ParquetObjectReader, GeoParquetReaderMetadata)>,
//     > = HashMap::new();
//     for ((path, reader), arrow_meta) in paths.iter().zip(readers).zip(parquet_metas) {
//         let geoparquet_meta = GeoParquetReaderMetadata::new(arrow_meta);
//         let value = (reader, geoparquet_meta);
//         if let Some(items) = hashmap.get_mut(path) {
//             items.push(value);
//         } else {
//             hashmap.insert(path.clone(), vec![value]);
//         }
//     }

//     Ok(hashmap)
// }

/// Create a reader per path with the given ObjectStore instance.
// TODO: deduplicate with JS binding
async fn fetch_arrow_metadata_objects(
    paths: Vec<String>,
    store: Arc<dyn ObjectStore>,
) -> Result<HashMap<String, ArrowReaderMetadata>, GeoArrowError> {
    let paths: Vec<object_store::path::Path> = paths.into_iter().map(|path| path.into()).collect();
    let object_meta_futures = paths.iter().map(|path| store.head(path));
    let object_metas = futures::future::join_all(object_meta_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, object_store::Error>>()
        .map_err(GeoArrowError::ObjectStoreError)?;
    let mut readers = object_metas
        .into_iter()
        .map(|meta| ParquetObjectReader::new(store.clone(), meta))
        .collect::<Vec<_>>();
    let parquet_meta_futures = readers
        .iter_mut()
        .map(|reader| ArrowReaderMetadata::load_async(reader, Default::default()));
    let parquet_metas = futures::future::join_all(parquet_meta_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, parquet::errors::ParquetError>>()
        .map_err(GeoArrowError::ParquetError)?;

    let mut hashmap: HashMap<String, ArrowReaderMetadata> = HashMap::new();
    for (path, arrow_meta) in paths.iter().zip(parquet_metas) {
        hashmap.insert(path.to_string(), arrow_meta);
    }

    Ok(hashmap)
}

/// Encapsulates details of reading a complete Parquet dataset possibly consisting of multiple
/// files and partitions in subdirectories.
#[pyclass(module = "geoarrow.rust.io", frozen)]
pub struct ParquetDataset {
    meta: GeoParquetDatasetMetadata,
    // metas: HashMap<object_store::path::Path, Vec<(ParquetObjectReader, GeoParquetReaderMetadata)>>,
    store: Arc<dyn ObjectStore>,
}

impl ParquetDataset {
    fn to_readers(
        &self,
        geo_options: GeoParquetReaderOptions,
    ) -> Result<Vec<GeoParquetRecordBatchStream<ParquetObjectReader>>, GeoArrowError> {
        self.meta
            .to_stream_builders(
                |path| {
                    let object_meta = ObjectMeta {
                        location: path.into(),
                        last_modified: Default::default(),
                        // NOTE: Usually we'd need to know the file size of each object, but since
                        // we already have the Parquet metadata, this should be ok
                        size: 0,
                        e_tag: None,
                        version: None,
                    };
                    ParquetObjectReader::new(self.store.clone(), object_meta)
                },
                geo_options,
            )
            .into_iter()
            .map(|builder| builder.build())
            .collect()
    }

    async fn read_inner(
        readers: Vec<GeoParquetRecordBatchStream<ParquetObjectReader>>,
        output_schema: SchemaRef,
    ) -> PyGeoArrowResult<Arro3Table> {
        let request_futures = readers.into_iter().map(|reader| reader.read_table());
        let tables = futures::future::join_all(request_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, GeoArrowError>>()
            .map_err(PyGeoArrowError::GeoArrowError)?;

        let mut all_batches = vec![];
        tables.into_iter().for_each(|table| {
            let (table_batches, _schema) = table.into_inner();
            all_batches.extend(table_batches);
        });
        let table =
            Table::try_new(all_batches, output_schema).map_err(PyGeoArrowError::GeoArrowError)?;
        Ok(to_arro3_table(table))
    }
}

#[pymethods]
impl ParquetDataset {
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

    #[getter]
    fn schema_arrow(&self) -> PyGeoArrowResult<Arro3Schema> {
        let schema = self.meta.resolved_schema(Default::default())?;
        Ok(schema.into())
    }

    #[pyo3(signature = (column_name=None))]
    fn crs(&self, py: Python, column_name: Option<&str>) -> PyGeoArrowResult<PyObject> {
        if let Some(crs) = self.meta.crs(column_name)? {
            Ok(CRS::from_projjson(crs.clone())
                .to_pyproj(py)
                .map_err(PyErr::from)?)
        } else {
            Ok(py.None())
        }
    }

    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read_async<'py>(
        &self,
        py: Python<'py>,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<PyGeoParquetBboxCovering>,
    ) -> PyGeoArrowResult<Bound<'py, PyAny>> {
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let readers = self.to_readers(options)?;
        let output_schema = self.meta.resolved_schema(Default::default())?;

        let fut = future_into_py(py, async move {
            Ok(Self::read_inner(readers, output_schema).await?)
        })?;
        Ok(fut)
    }

    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<PyGeoParquetBboxCovering>,
    ) -> PyGeoArrowResult<Arro3Table> {
        let runtime = get_runtime(py)?;
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let readers = self.to_readers(options)?;
        let output_schema = self.meta.resolved_schema(Default::default())?;

        runtime.block_on(Self::read_inner(readers, output_schema))
    }
}
