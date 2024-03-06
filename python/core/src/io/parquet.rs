use std::fs::File;
use std::io::BufWriter;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::table::GeoTable;

use geoarrow::array::CoordType;
use geoarrow::error::GeoArrowError;
use geoarrow::io::parquet::read_geoparquet as _read_geoparquet;
use geoarrow::io::parquet::write_geoparquet as _write_geoparquet;
use geoarrow::io::parquet::GeoParquetReaderOptions;
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

    let options = GeoParquetReaderOptions::new(batch_size, Default::default());
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

#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct ParquetFile {
    file: _ParquetFile<ParquetObjectReader>,
}

#[pymethods]
impl ParquetFile {
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

    /// Access the bounding box of the given column for the entire file
    ///
    /// If no column name is passed, retrieves the bbox from the primary geometry column.
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    fn bbox(&self, column_name: Option<&str>) -> PyGeoArrowResult<Option<Vec<f64>>> {
        let bbox = self.file.file_bbox(column_name)?;
        Ok(bbox.map(|b| b.to_vec()))
    }

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
}
