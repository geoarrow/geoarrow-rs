use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::sync::FileWriter;
use crate::io::input::{construct_reader, AnyFileReader};
use crate::util::table_to_pytable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use geoarrow::io::flatgeobuf::{read_flatgeobuf as _read_flatgeobuf, FlatGeobufReaderOptions};
use pyo3::prelude::*;
use pyo3_arrow::input::AnyRecordBatch;

#[pyfunction]
#[pyo3(signature = (file, *, fs=None, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf(
    py: Python,
    file: PyObject,
    fs: Option<PyObject>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(py, file, fs)?;
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

                Ok(table_to_pytable(table).to_arro3(py)?)
            })
        }
        AnyFileReader::Sync(mut sync_reader) => {
            let options = FlatGeobufReaderOptions {
                batch_size: Some(batch_size),
                bbox,
                ..Default::default()
            };
            let table = _read_flatgeobuf(&mut sync_reader, options)?;
            Ok(table_to_pytable(table).to_arro3(py)?)
        }
    }
}

#[pyfunction]
#[pyo3(signature = (table, file, *, write_index=true))]
pub fn write_flatgeobuf(
    py: Python,
    table: AnyRecordBatch,
    file: FileWriter,
    write_index: bool,
) -> PyGeoArrowResult<()> {
    let name = file.file_stem(py);

    let options = FgbWriterOptions {
        write_index,
        ..Default::default()
    };
    _write_flatgeobuf(
        table.into_reader()?,
        file,
        name.as_deref().unwrap_or(""),
        options,
    )?;
    Ok(())
}
