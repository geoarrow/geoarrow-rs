use std::fs::File;
use std::sync::Arc;

use crate::array::PolygonArray;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::interop::util::table_to_pytable;
use crate::io::input::sync::BinaryFileReader;
use crate::io::input::{construct_reader, FileReader};
use crate::io::object_store::PyObjectStore;
use crate::io::parquet::options::{create_options, GeoParquetBboxPaths};

use geoarrow::error::GeoArrowError;
use geoarrow::geo_traits::{CoordTrait, RectTrait};
use geoarrow::io::parquet::GeoParquetReaderOptions;
use geoarrow::io::parquet::GeoParquetRecordBatchReaderBuilder;
use geoarrow::io::parquet::GeoParquetRecordBatchStreamBuilder;
use geoarrow::io::parquet::ParquetDataset as _ParquetDataset;
use geoarrow::io::parquet::ParquetFile as _ParquetFile;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// Read a GeoParquet file from a path on disk into a GeoTable.
///
/// Example:
///
/// Reading from a local path:
///
/// ```py
/// from geoarrow.rust.core import read_parquet
/// table = read_parquet("path/to/file.parquet")
/// ```
///
/// Reading from an HTTP(S) url:
///
/// ```py
/// from geoarrow.rust.core import read_parquet
///
/// url = "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples/example.parquet"
/// table = read_parquet(url)
/// ```
///
/// Reading from a remote file on an S3 bucket.
///
/// ```py
/// from geoarrow.rust.core import ObjectStore, read_parquet
///
/// options = {
///     "aws_access_key_id": "...",
///     "aws_secret_access_key": "...",
///     "aws_region": "..."
/// }
/// fs = ObjectStore('s3://bucket', options=options)
/// table = read_parquet("path/in/bucket.parquet", fs=fs)
/// ```
///
/// Args:
///     path: the path to the file
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from GeoParquet file.
#[pyfunction]
#[pyo3(signature = (path, *, fs=None, batch_size=None))]
pub fn read_parquet(
    py: Python,
    path: PyObject,
    fs: Option<PyObjectStore>,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(py, path, fs)?;
    match reader {
        FileReader::Async(async_reader) => {
            let table = async_reader.runtime.block_on(async move {
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

                let table = GeoParquetRecordBatchStreamBuilder::new_with_options(
                    reader,
                    Default::default(),
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
        FileReader::Sync(sync_reader) => match sync_reader {
            BinaryFileReader::String(path, _) => {
                let file = File::open(path)
                    .map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;

                let mut geo_options = GeoParquetReaderOptions::default();

                if let Some(batch_size) = batch_size {
                    geo_options = geo_options.with_batch_size(batch_size);
                }

                let table = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
                    file,
                    Default::default(),
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

/// Read a GeoParquet file from a path on disk into a GeoTable.
///
/// Examples:
///
/// Reading from an HTTP(S) url:
///
/// ```py
/// from geoarrow.rust.core import read_parquet_async
///
/// url = "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples/example.parquet"
/// table = await read_parquet_async(url)
/// ```
///
/// Reading from a remote file on an S3 bucket.
///
/// ```py
/// from geoarrow.rust.core import ObjectStore, read_parquet_async
///
/// options = {
///     "aws_access_key_id": "...",
///     "aws_secret_access_key": "...",
///     "aws_region": "..."
/// }
/// fs = ObjectStore('s3://bucket', options=options)
/// table = await read_parquet_async("path/in/bucket.parquet", fs=fs)
/// ```
///
/// Args:
///     path: the path to the file
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from GeoParquet file.
#[pyfunction]
#[pyo3(signature = (path, *, fs=None, batch_size=None))]
pub fn read_parquet_async(
    py: Python,
    path: PyObject,
    fs: Option<PyObjectStore>,
    batch_size: Option<usize>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(py, path, fs)?;
    match reader {
        FileReader::Async(async_reader) => {
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

                let table = GeoParquetRecordBatchStreamBuilder::new_with_options(
                    reader,
                    Default::default(),
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
        FileReader::Sync(_) => {
            Err(PyValueError::new_err("Local file paths not supported in async reader.").into())
        }
    }
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetFile {
    file: _ParquetFile<ParquetObjectReader>,
    rt: Arc<Runtime>,
}

#[pymethods]
impl ParquetFile {
    /// Construct a new ParquetFile
    ///
    /// This will synchronously fetch metadata from the provided path
    ///
    /// Args:
    ///     path: a string URL to read from.
    ///     fs: the file system interface to read from.
    ///
    /// Returns:
    ///     A new ParquetFile object.
    // TODO: change this to aenter
    #[new]
    pub fn new(path: String, fs: PyObjectStore) -> PyGeoArrowResult<Self> {
        let file = fs.rt.block_on(async move {
            let meta = fs
                .inner
                .head(&path.into())
                .await
                .map_err(GeoArrowError::ObjectStoreError)?;
            let reader = ParquetObjectReader::new(fs.inner, meta);
            let file = _ParquetFile::new(reader).await?;
            Ok::<_, PyGeoArrowError>(file)
        })?;
        Ok(Self {
            file,
            rt: fs.rt.clone(),
        })
    }

    /// The number of rows in this file.
    #[getter]
    fn num_rows(&self) -> usize {
        self.file.num_rows()
    }

    /// The number of row groups in this file.
    #[getter]
    fn num_row_groups(&self) -> usize {
        self.file.num_row_groups()
    }

    /// Get the bounds of a single row group.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_group_bounds(
        &self,
        minx_path: Vec<String>,
        miny_path: Vec<String>,
        maxx_path: Vec<String>,
        maxy_path: Vec<String>,
        row_group_idx: usize,
    ) -> PyGeoArrowResult<Option<Vec<f64>>> {
        let paths = geoarrow::io::parquet::ParquetBboxPaths {
            minx_path,
            miny_path,
            maxx_path,
            maxy_path,
        };

        if let Some(bounds) = self.file.row_group_bounds(&paths, row_group_idx)? {
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

    /// Get the bounds of all row groups.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_groups_bounds(
        &self,
        minx_path: Vec<String>,
        miny_path: Vec<String>,
        maxx_path: Vec<String>,
        maxy_path: Vec<String>,
    ) -> PyGeoArrowResult<PolygonArray> {
        let paths = geoarrow::io::parquet::ParquetBboxPaths {
            minx_path,
            miny_path,
            maxx_path,
            maxy_path,
        };
        let bounds = self.file.row_groups_bounds(&paths)?;
        Ok(bounds.into())
    }

    /// Access the bounding box of the given column for the entire file
    ///
    /// If no column name is passed, retrieves the bbox from the primary geometry column.
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    fn file_bbox(&self, column_name: Option<&str>) -> PyGeoArrowResult<Option<Vec<f64>>> {
        let bbox = self.file.file_bbox(column_name)?;
        Ok(bbox.map(|b| b.to_vec()))
    }

    /// Perform an async read with the given options
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read_async(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<GeoParquetBboxPaths>,
    ) -> PyGeoArrowResult<PyObject> {
        let file = self.file.clone();
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths);
        let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let table = file
                .read(options)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table))
        })?;
        Ok(fut.into())
    }

    /// Perform a sync read with the given options
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<GeoParquetBboxPaths>,
    ) -> PyGeoArrowResult<PyObject> {
        let file = self.file.clone();
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths);
        self.rt.block_on(async move {
            let table = file
                .read(options)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table).to_arro3(py)?)
        })
    }

    /// Read the selected row group indexes in an async fashion.
    ///
    /// Args:
    ///     row_groups: numeric indexes of the Parquet row groups to read.
    ///
    /// Returns:
    ///     parsed table.
    fn read_row_groups_async(
        &self,
        py: Python,
        row_groups: Vec<usize>,
    ) -> PyGeoArrowResult<PyObject> {
        let file = self.file.clone();
        let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let table = file
                .read_row_groups(row_groups, Default::default())
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table))
        })?;
        Ok(fut.into())
    }

    /// Read the selected row group indexes synchronously.
    ///
    /// Args:
    ///     row_groups: numeric indexes of the Parquet row groups to read.
    ///
    /// Returns:
    ///     parsed table.
    fn read_row_groups(&self, py: Python, row_groups: Vec<usize>) -> PyGeoArrowResult<PyObject> {
        let file = self.file.clone();
        self.rt.block_on(async move {
            let table = file
                .read_row_groups(row_groups, Default::default())
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table).to_arro3(py)?)
        })
    }
}

/// Encapsulates details of reading a complete Parquet dataset possibly consisting of multiple
/// files and partitions in subdirectories.
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetDataset {
    inner: _ParquetDataset<ParquetObjectReader>,
    rt: Arc<Runtime>,
}

/// Create a reader per path with the given ObjectStore instance.
async fn create_readers(
    paths: Vec<String>,
    store: Arc<dyn ObjectStore>,
) -> PyGeoArrowResult<Vec<ParquetObjectReader>> {
    let paths: Vec<object_store::path::Path> = paths.into_iter().map(|path| path.into()).collect();
    let futures = paths.iter().map(|path| store.head(path));
    let object_metas = futures::future::join_all(futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, object_store::Error>>()
        .map_err(GeoArrowError::ObjectStoreError)?;
    let readers = object_metas
        .into_iter()
        .map(|meta| ParquetObjectReader::new(store.clone(), meta))
        .collect::<Vec<_>>();
    Ok(readers)
}

#[pymethods]
impl ParquetDataset {
    /// Construct a new ParquetDataset
    ///
    /// This will synchronously fetch metadata from all listed files.
    ///
    /// Args:
    ///     paths: a list of string URLs to read from.
    ///     fs: the file system interface to read from.
    ///
    /// Returns:
    ///     A new ParquetDataset object.
    #[new]
    pub fn new(paths: Vec<String>, fs: PyObjectStore) -> PyGeoArrowResult<Self> {
        let dataset = fs.rt.block_on(async move {
            let readers = create_readers(paths, fs.inner).await?;
            let inner = _ParquetDataset::new(readers).await?;
            Ok::<_, PyGeoArrowError>(inner)
        })?;
        Ok(Self {
            inner: dataset,
            rt: fs.rt.clone(),
        })
    }

    /// The total number of rows across all files.
    #[getter]
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// The total number of row groups across all files
    #[getter]
    fn num_row_groups(&self) -> usize {
        self.inner.num_row_groups()
    }

    /// Read this entire file in an async fashion.
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read_async(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<GeoParquetBboxPaths>,
    ) -> PyGeoArrowResult<PyObject> {
        let inner = self.inner.clone();
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths);
        let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let table = inner
                .read(options)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table))
        })?;
        Ok(fut.into())
    }

    /// Read this entire file synchronously.
    #[pyo3(signature = (*, batch_size=None, limit=None, offset=None, bbox=None, bbox_paths=None))]
    fn read(
        &self,
        py: Python,
        batch_size: Option<usize>,
        limit: Option<usize>,
        offset: Option<usize>,
        bbox: Option<[f64; 4]>,
        bbox_paths: Option<GeoParquetBboxPaths>,
    ) -> PyGeoArrowResult<PyObject> {
        let inner = self.inner.clone();
        let options = create_options(batch_size, limit, offset, bbox, bbox_paths);
        self.rt.block_on(async move {
            let table = inner
                .read(options)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(table_to_pytable(table).to_arro3(py)?)
        })
    }
}
