use std::collections::HashMap;
use std::sync::Arc;

use crate::crs::CRS;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::{construct_reader, AnyFileReader};
use crate::io::object_store::PyObjectStore;
use crate::io::parquet::options::create_options;
use crate::util::table_to_pytable;

use geoarrow::error::GeoArrowError;
use geoarrow::geo_traits::{CoordTrait, RectTrait};
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
use pyo3_arrow::{PyArray, PySchema};
use pythonize::depythonize_bound;
use tokio::runtime::Runtime;

#[pyfunction]
#[pyo3(signature = (path, *, fs=None, batch_size=None))]
pub fn read_parquet_async(
    py: Python,
    path: PyObject,
    fs: Option<PyObject>,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(py, path, fs)?;
    match reader {
        AnyFileReader::Async(async_reader) => {
            let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
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
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?
                .build()
                .map_err(PyGeoArrowError::GeoArrowError)?
                .read_table()
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;

                Ok(table_to_pytable(table))
            })?;
            Ok(fut.into())
        }
        AnyFileReader::Sync(_) => {
            Err(PyValueError::new_err("Local file paths not supported in async reader.").into())
        }
    }
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "geoarrow.rust.io._io")]
pub struct ParquetFile {
    object_meta: object_store::ObjectMeta,
    geoparquet_meta: GeoParquetReaderMetadata,
    store: Arc<dyn ObjectStore>,
    rt: Arc<Runtime>,
}

#[pymethods]
impl ParquetFile {
    // TODO: change this to aenter
    #[new]
    pub fn new(path: String, fs: PyObjectStore) -> PyGeoArrowResult<Self> {
        let store = fs.inner.clone();
        let (object_meta, geoparquet_meta) = fs.rt.block_on(async move {
            let object_meta = fs
                .inner
                .head(&path.into())
                .await
                .map_err(GeoArrowError::ObjectStoreError)?;
            let mut reader = ParquetObjectReader::new(fs.inner.clone(), object_meta.clone());
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
            rt: fs.rt.clone(),
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
    fn schema_arrow(&self, py: Python) -> PyGeoArrowResult<PyObject> {
        let schema = self.geoparquet_meta.resolved_schema(Default::default())?;
        Ok(PySchema::new(schema).to_arro3(py)?)
    }

    fn crs(&self, py: Python, column_name: Option<&str>) -> PyGeoArrowResult<PyObject> {
        if let Some(crs) = self.geoparquet_meta.crs(column_name)? {
            // TODO: remove clone
            CRS::new(crs.clone()).to_pyproj(py)
        } else {
            Ok(py.None())
        }
    }

    pub fn row_group_bounds(
        &self,
        row_group_idx: usize,
        bbox_paths: Option<Bound<'_, PyAny>>,
    ) -> PyGeoArrowResult<Option<Vec<f64>>> {
        let paths: Option<GeoParquetBboxCovering> =
            bbox_paths.map(|x| depythonize_bound(x)).transpose()?;

        if let Some(bounds) = self
            .geoparquet_meta
            .row_group_bounds(row_group_idx, paths.as_ref())?
        {
            Ok(Some(vec![
                bounds.lower().x(),
                bounds.lower().y(),
                bounds.upper().x(),
                bounds.upper().y(),
            ]))
        } else {
            Ok(None)
        }
    }

    pub fn row_groups_bounds(
        &self,
        py: Python,
        bbox_paths: Option<Bound<'_, PyAny>>,
    ) -> PyGeoArrowResult<PyObject> {
        let paths: Option<GeoParquetBboxCovering> =
            bbox_paths.map(|x| depythonize_bound(x)).transpose()?;
        let bounds = self.geoparquet_meta.row_groups_bounds(paths.as_ref())?;
        Ok(PyArray::new(bounds.to_array_ref(), bounds.extension_field()).to_arro3(py)?)
    }

    fn file_bbox(&self, column_name: Option<&str>) -> PyGeoArrowResult<Option<Vec<f64>>> {
        let bbox = self.geoparquet_meta.file_bbox(column_name)?;
        Ok(bbox.map(|b| b.to_vec()))
    }

    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read_async(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<Bound<'_, PyAny>>,
    ) -> PyGeoArrowResult<PyObject> {
        let reader = ParquetObjectReader::new(self.store.clone(), self.object_meta.clone());
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let stream = GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
            reader,
            self.geoparquet_meta.clone(),
            options,
        )
        .build()?;
        let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let table = stream
                .read_table()
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table))
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
        bbox_paths: Option<Bound<'_, PyAny>>,
    ) -> PyGeoArrowResult<PyObject> {
        let reader = ParquetObjectReader::new(self.store.clone(), self.object_meta.clone());
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let stream = GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
            reader,
            self.geoparquet_meta.clone(),
            options,
        )
        .build()?;
        self.rt.block_on(async move {
            let table = stream
                .read_table()
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table).to_arro3(py)?)
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
#[pyclass(module = "geoarrow.rust.io._io")]
pub struct ParquetDataset {
    meta: GeoParquetDatasetMetadata,
    // metas: HashMap<object_store::path::Path, Vec<(ParquetObjectReader, GeoParquetReaderMetadata)>>,
    store: Arc<dyn ObjectStore>,
    rt: Arc<Runtime>,
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
                        // NOTE: Usually we'd need to know the file size of each object, but since we
                        // already have the Parquet metadata, this should be ok
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
}

#[pymethods]
impl ParquetDataset {
    #[new]
    pub fn new(paths: Vec<String>, fs: PyObjectStore) -> PyGeoArrowResult<Self> {
        let store = fs.inner.clone();
        let meta = fs.rt.block_on(async move {
            let meta = fetch_arrow_metadata_objects(paths, fs.inner).await?;
            // let metas = fetch_geoparquet_metas(paths, fs.inner).await?;
            Ok::<_, PyGeoArrowError>(meta)
        })?;

        Ok(Self {
            meta: GeoParquetDatasetMetadata::from_files(meta)?,
            store,
            rt: fs.rt.clone(),
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
    fn schema_arrow(&self, py: Python) -> PyGeoArrowResult<PyObject> {
        let schema = self.meta.resolved_schema(Default::default())?;
        Ok(PySchema::new(schema).to_arro3(py)?)
    }

    fn crs(&self, py: Python, column_name: Option<&str>) -> PyGeoArrowResult<PyObject> {
        if let Some(crs) = self.meta.crs(column_name)? {
            // TODO: remove clone
            CRS::new(crs.clone()).to_pyproj(py)
        } else {
            Ok(py.None())
        }
    }

    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read_async(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<Bound<'_, PyAny>>,
    ) -> PyGeoArrowResult<PyObject> {
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let readers = self.to_readers(options)?;
        let output_schema = self.meta.resolved_schema(Default::default())?;

        let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
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
            let table = Table::try_new(all_batches, output_schema)
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table))
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
        bbox_paths: Option<Bound<'_, PyAny>>,
    ) -> PyGeoArrowResult<PyObject> {
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths)?;
        let readers = self.to_readers(options)?;
        let output_schema = self.meta.resolved_schema(Default::default())?;

        self.rt.block_on(async move {
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
            let table = Table::try_new(all_batches, output_schema)
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table).to_arro3(py)?)
        })
    }
}
