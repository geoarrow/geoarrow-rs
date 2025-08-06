use std::io::{Seek, SeekFrom};

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
#[cfg(feature = "async")]
use crate::input::AnyFileReader;
use crate::input::construct_reader;
use crate::input::sync::FileWriter;

use arrow::array::RecordBatchReader;
use flatgeobuf::FgbReader;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchIterator,
};
use geoarrow_flatgeobuf::writer::FlatGeobufWriterOptions;
use geoarrow_flatgeobuf::writer::write_flatgeobuf_with_options as _write_flatgeobuf;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;
use pyo3_arrow::export::Arro3Table;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_geoarrow::{PyCoordType, PyprojCRSTransform};

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, *, store=None, batch_size=65536, bbox=None, coord_type=None, prefer_view_types=true, max_read_records=Some(1000), read_geometry=true))]
pub fn read_flatgeobuf(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
    coord_type: Option<PyCoordType>,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
    read_geometry: bool,
) -> PyGeoArrowResult<Arro3Table> {
    let reader = construct_reader(path, store)?;
    let coord_type = coord_type.map(|x| x.into()).unwrap_or_default();
    match reader {
        #[cfg(feature = "async")]
        AnyFileReader::Async(async_reader) => {
            use crate::runtime::get_runtime;
            use flatgeobuf::HttpFgbReader;
            use geoarrow_flatgeobuf::reader::object_store::ObjectStoreWrapper;
            use http_range_client::AsyncBufferedHttpRangeClient;

            let runtime = get_runtime(py)?;

            runtime.block_on(async move {
                use futures::TryStreamExt;
                use geoarrow_flatgeobuf::reader::FlatGeobufRecordBatchStream;

                let object_store_wrapper =
                    ObjectStoreWrapper::new(async_reader.store, async_reader.path);
                let async_client =
                    AsyncBufferedHttpRangeClient::with(object_store_wrapper.clone(), "");
                let fgb_reader = HttpFgbReader::new(async_client).await?;
                let fgb_header = fgb_reader.header();

                let properties_schema = if let Some(properties_schema) =
                    fgb_header.properties_schema(prefer_view_types)
                {
                    properties_schema
                } else {
                    let async_scan_client =
                        AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
                    let fgb_reader_scan = HttpFgbReader::new(async_scan_client).await?;
                    let mut scanner = FlatGeobufSchemaScanner::new(prefer_view_types);
                    scanner
                        .process_async(fgb_reader_scan.select_all().await?, max_read_records)
                        .await?;
                    scanner.finish()
                };

                let geometry_type = fgb_header.geoarrow_type(coord_type)?;
                let selection = if let Some(bbox) = bbox {
                    fgb_reader
                        .select_bbox(bbox.0, bbox.1, bbox.2, bbox.3)
                        .await?
                } else {
                    fgb_reader.select_all().await?
                };

                let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type)
                    .with_batch_size(batch_size)
                    .with_read_geometry(read_geometry);
                let record_batch_stream = FlatGeobufRecordBatchStream::try_new(selection, options)?;
                let schema = record_batch_stream.schema();
                let batches = record_batch_stream.try_collect::<Vec<_>>().await?;

                let table = Arro3Table::from(PyTable::try_new(batches, schema)?);
                Ok::<_, PyGeoArrowError>(table)
            })
        }
        AnyFileReader::Sync(mut sync_reader) => {
            let fgb_reader = FgbReader::open(sync_reader.try_clone()?)?;
            let fgb_header = fgb_reader.header();

            let properties_schema =
                if let Some(properties_schema) = fgb_header.properties_schema(prefer_view_types) {
                    properties_schema
                } else {
                    // try_clone doesn't fully clone the file handle. We need to seek back to the
                    // original position after reading the schema.
                    let pos = sync_reader.stream_position()?;
                    let fgb_reader_scan = FgbReader::open(sync_reader.try_clone()?)?;
                    let mut scanner = FlatGeobufSchemaScanner::new(prefer_view_types);
                    scanner.process(fgb_reader_scan.select_all()?, max_read_records)?;

                    sync_reader.seek(SeekFrom::Start(pos))?;
                    scanner.finish()
                };

            let geometry_type = fgb_header.geoarrow_type(coord_type)?;
            let selection = if let Some(bbox) = bbox {
                fgb_reader.select_bbox(bbox.0, bbox.1, bbox.2, bbox.3)?
            } else {
                fgb_reader.select_all()?
            };

            let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type)
                .with_batch_size(batch_size)
                .with_read_geometry(read_geometry);
            let record_batch_reader = FlatGeobufRecordBatchIterator::try_new(selection, options)?;
            let schema = record_batch_reader.schema();
            let batches = record_batch_reader.collect::<Result<Vec<_>, _>>()?;

            Ok(Arro3Table::from(PyTable::try_new(batches, schema)?))
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
) -> PyGeoArrowResult<()> {
    let name = name.unwrap_or_else(|| file.file_stem(py).unwrap_or("".to_string()));
    let options = FlatGeobufWriterOptions {
        write_index,
        title,
        description,
        metadata,
        crs_transform: Some(Box::new(PyprojCRSTransform::new())),
        ..Default::default()
    };
    _write_flatgeobuf(table.into_reader()?, file, &name, options)?;
    Ok(())
}
