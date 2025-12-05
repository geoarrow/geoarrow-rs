use geo::{Rect, coord};
use geoparquet::metadata::GeoParquetMetadata;
use geoparquet::reader::GeoParquetReaderBuilder;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use pyo3::prelude::*;

use crate::error::PyGeoArrowResult;

#[derive(Debug, Clone, Copy)]
pub struct PyRect(Rect<f64>);

impl<'a, 'py> FromPyObject<'a, 'py> for PyRect {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let bbox = ob.extract::<[f64; 4]>()?;
        Ok(Self(Rect::new(
            coord! {x: bbox[0], y: bbox[1]},
            coord! {x: bbox[2], y: bbox[3]},
        )))
    }
}

#[derive(Debug, Clone)]
pub struct PyGeoParquetBboxQuery {
    pub bbox: PyRect,
    pub column_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PyGeoParquetReadOptions {
    pub batch_size: Option<usize>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub bbox_query: Option<PyGeoParquetBboxQuery>,
}

pub fn apply_options<T>(
    mut builder: ArrowReaderBuilder<T>,
    geo_metadata: &GeoParquetMetadata,
    options: PyGeoParquetReadOptions,
) -> PyGeoArrowResult<ArrowReaderBuilder<T>> {
    if let Some(batch_size) = options.batch_size {
        builder = builder.with_batch_size(batch_size);
    }
    if let Some(limit) = options.limit {
        builder = builder.with_limit(limit);
    }
    if let Some(offset) = options.offset {
        builder = builder.with_offset(offset);
    }

    if let Some(PyGeoParquetBboxQuery { bbox, column_name }) = options.bbox_query {
        builder =
            builder.with_intersecting_row_groups(bbox.0, geo_metadata, column_name.as_deref())?;
        builder =
            builder.with_intersecting_row_filter(bbox.0, geo_metadata, column_name.as_deref())?;
    }

    Ok(builder)
}
