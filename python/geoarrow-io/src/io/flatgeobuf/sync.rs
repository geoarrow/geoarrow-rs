use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::sync::FileWriter;
use crate::io::input::{AnyFileReader, construct_reader};
use crate::util::to_arro3_table;

use arrow::array::RecordBatchReader;
use geoarrow::io::flatgeobuf::{FlatGeobufReaderBuilder, FlatGeobufReaderOptions};
use geoarrow::table::Table;
use geoarrow_flatgeobuf::writer::{
    FlatGeobufWriterOptions, write_flatgeobuf_with_options as _write_flatgeobuf,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;
use pyo3_arrow::input::AnyRecordBatch;

#[pyfunction]
#[pyo3(signature = (file, *, store=None, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf(
    py: Python,
    file: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<Arro3Table> {
    let reader = construct_reader(file, store)?;
    match reader {
        #[cfg(feature = "async")]
        AnyFileReader::Async(async_reader) => {
            use crate::runtime::get_runtime;

            let runtime = get_runtime(py)?;

            runtime.block_on(async move {
                use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;

                let options = FlatGeobufReaderOptions {
                    batch_size: Some(batch_size),
                    bbox,
                    ..Default::default()
                };
                let table = _read_flatgeobuf_async(async_reader.store, async_reader.path, options)
                    .await
                    .map_err(PyGeoArrowError::GeoArrowError)?;

                Ok(to_arro3_table(table))
            })
        }
        AnyFileReader::Sync(sync_reader) => {
            let options = FlatGeobufReaderOptions {
                batch_size: Some(batch_size),
                bbox,
                ..Default::default()
            };
            let reader_builder = FlatGeobufReaderBuilder::open(sync_reader)?;
            let reader = reader_builder.read(options)?;
            let table = Table::try_from(Box::new(reader) as Box<dyn RecordBatchReader>).unwrap();
            Ok(to_arro3_table(table))
        }
    }
}

#[pyfunction]
#[pyo3(signature = (
    table,
    file,
    *,
    write_index=true,
    title=None,
    description=None,
    metadata=None,
    name=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn write_flatgeobuf(
    py: Python,
    table: AnyRecordBatch,
    file: FileWriter,
    write_index: bool,
    title: Option<String>,
    description: Option<String>,
    metadata: Option<String>,
    name: Option<String>,
    // NOTE: restore PyGeoArrowResult
) -> PyResult<()> {
    let name = name.unwrap_or_else(|| file.file_stem(py).unwrap_or("".to_string()));

    let options = FlatGeobufWriterOptions {
        write_index,
        title,
        description,
        metadata,
        // Use pyproj for converting CRS to WKT
        // TODO: during the refactor of FlatGeobuf to use the new geoarrow-flatgeobuf crate, we
        // took out this CRS transform functionality because of conflicting trait definitions in
        // multiple crates.
        // crs_transform: Some(Box::new(PyprojCRSTransform::new())),
        ..Default::default()
    };

    _write_flatgeobuf(table.into_reader()?, file, &name, options)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok(())
}
