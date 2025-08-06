use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::input::{AsyncFileReader, construct_async_reader};

use flatgeobuf::HttpFgbReader;
use futures::TryStreamExt;
use geoarrow_flatgeobuf::reader::object_store::ObjectStoreWrapper;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchStream,
};
use geoarrow_schema::CoordType;
use http_range_client::AsyncBufferedHttpRangeClient;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;
use pyo3_arrow::export::Arro3Table;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_geoarrow::PyCoordType;

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (path, *, store=None, batch_size=65536, bbox=None, coord_type=None, prefer_view_types=true, max_read_records=Some(1000), read_geometry=true))]
pub fn read_flatgeobuf_async<'py>(
    py: Python<'py>,
    path: Bound<'py, PyAny>,
    store: Option<Bound<'py, PyAny>>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
    coord_type: Option<PyCoordType>,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
    read_geometry: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let async_reader = construct_async_reader(path, store)?;
    let coord_type = coord_type.map(|x| x.into()).unwrap_or_default();
    future_into_py(py, async move {
        Ok(read_flatgeobuf_async_inner(
            async_reader,
            batch_size,
            bbox,
            coord_type,
            prefer_view_types,
            max_read_records,
            read_geometry,
        )
        .await?)
    })
}

async fn read_flatgeobuf_async_inner(
    async_reader: AsyncFileReader,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
    coord_type: CoordType,
    prefer_view_types: bool,
    max_read_records: Option<usize>,
    read_geometry: bool,
) -> PyGeoArrowResult<Arro3Table> {
    let object_store_wrapper = ObjectStoreWrapper::new(async_reader.store, async_reader.path);
    let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper.clone(), "");
    let fgb_reader = HttpFgbReader::new(async_client).await?;
    let fgb_header = fgb_reader.header();

    let properties_schema =
        if let Some(properties_schema) = fgb_header.properties_schema(prefer_view_types) {
            properties_schema
        } else {
            let async_scan_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");
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
}
