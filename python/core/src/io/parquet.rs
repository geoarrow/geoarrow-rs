use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use crate::array::PolygonArray;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::table::GeoTable;

use geoarrow::array::CoordType;
use geoarrow::error::GeoArrowError;
use geoarrow::io::parquet::read_geoparquet as _read_geoparquet;
use geoarrow::io::parquet::write_geoparquet as _write_geoparquet;
use geoarrow::io::parquet::GeoParquetReaderOptions;
use geoarrow::io::parquet::ParquetDataset as _ParquetDataset;
use geoarrow::io::parquet::ParquetFile as _ParquetFile;
use object_store::ObjectStore;
use object_store_python::PyObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a GeoParquet file from a path on disk into a GeoTable.
///
/// Args:
///     path: the path to the file
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from GeoParquet file.
#[pyfunction]
#[pyo3(signature = (path, *, batch_size=65536))]
pub fn read_parquet(path: String, batch_size: usize) -> PyGeoArrowResult<GeoTable> {
    let file = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;

    let options = GeoParquetReaderOptions {
        batch_size,
        ..Default::default()
    };
    let table = _read_geoparquet(file, options)?;
    Ok(GeoTable(table))
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
pub fn write_parquet(mut table: GeoTable, file: String) -> PyGeoArrowResult<()> {
    let writer = BufWriter::new(
        File::create(file).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
    );

    _write_geoparquet(&mut table.0, writer, None)?;
    Ok(())
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetFile {
    file: _ParquetFile<ParquetObjectReader>,
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
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let meta = fs
                    .inner
                    .head(&path.into())
                    .await
                    .map_err(GeoArrowError::ObjectStoreError)?;
                let reader = ParquetObjectReader::new(fs.inner, meta);
                let file = _ParquetFile::new(reader, Default::default()).await?;
                Ok(Self { file })
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
        xmin_path: Vec<String>,
        ymin_path: Vec<String>,
        xmax_path: Vec<String>,
        ymax_path: Vec<String>,
        row_group_idx: usize,
    ) -> PyGeoArrowResult<Vec<f64>> {
        let bounds = self
            .file
            .row_group_bounds(
                xmin_path.as_slice(),
                ymin_path.as_slice(),
                xmax_path.as_slice(),
                ymax_path.as_slice(),
                row_group_idx,
            )?
            .unwrap();
        Ok(vec![
            bounds.minx(),
            bounds.miny(),
            bounds.maxx(),
            bounds.maxy(),
        ])
    }

    /// Get the bounds of all row groups.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_groups_bounds(
        &self,
        xmin_path: Vec<String>,
        ymin_path: Vec<String>,
        xmax_path: Vec<String>,
        ymax_path: Vec<String>,
    ) -> PyGeoArrowResult<PolygonArray> {
        let bounds = self.file.row_groups_bounds(
            xmin_path.as_slice(),
            ymin_path.as_slice(),
            xmax_path.as_slice(),
            ymax_path.as_slice(),
        )?;
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

    /// Read this entire file in an async fashion.
    fn read_async(&self, py: Python) -> PyGeoArrowResult<PyObject> {
        let file = self.file.clone();
        let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
            let table = file
                .read(&CoordType::Interleaved)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(GeoTable(table))
        })?;
        Ok(fut.into())
    }

    /// Read this entire file synchronously.
    fn read(&self) -> PyGeoArrowResult<GeoTable> {
        let file = self.file.clone();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let table = file
                    .read(&CoordType::Interleaved)
                    .await
                    .map_err(PyGeoArrowError::GeoArrowError)?;
                Ok(GeoTable(table))
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
        let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
            let table = file
                .read_row_groups(row_groups, &CoordType::Interleaved)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;
            Ok(GeoTable(table))
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
    fn read_row_groups(&self, row_groups: Vec<usize>) -> PyGeoArrowResult<GeoTable> {
        let file = self.file.clone();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let table = file
                    .read_row_groups(row_groups, &CoordType::Interleaved)
                    .await
                    .map_err(PyGeoArrowError::GeoArrowError)?;
                Ok(GeoTable(table))
            })
    }
}

/// Encapsulates details of reading a complete Parquet dataset possibly consisting of multiple
/// files and partitions in subdirectories.
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetDataset {
    inner: _ParquetDataset<ParquetObjectReader>,
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
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let readers = create_readers(paths, fs.inner).await?;
                let dataset = _ParquetDataset::new(readers, Default::default()).await?;
                Ok(Self { inner: dataset })
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
}
