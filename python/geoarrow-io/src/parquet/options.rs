use geo::coord;
use geoarrow_schema::CoordType;
use geoparquet::metadata::GeoParquetBboxCovering;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use pyo3::prelude::*;

use crate::error::PyGeoArrowResult;

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub struct PyGeoParquetBboxCovering {
    xmin: Vec<String>,
    ymin: Vec<String>,
    #[pyo3(default)]
    zmin: Option<Vec<String>>,
    xmax: Vec<String>,
    ymax: Vec<String>,
    #[pyo3(default)]
    zmax: Option<Vec<String>>,
}

impl From<PyGeoParquetBboxCovering> for GeoParquetBboxCovering {
    fn from(value: PyGeoParquetBboxCovering) -> Self {
        Self {
            xmin: value.xmin,
            ymin: value.ymin,
            zmin: value.zmin,
            xmax: value.xmax,
            ymax: value.ymax,
            zmax: value.zmax,
        }
    }
}

pub fn create_options(
    batch_size: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    // bbox: Option<[f64; 4]>,
    // bbox_paths: Option<PyGeoParquetBboxCovering>,
) -> PyGeoArrowResult<ArrowReaderOptions> {
    let bbox = bbox.map(|item| {
        geo::Rect::new(
            coord! {x: item[0], y: item[1]},
            coord! {x: item[2], y: item[3]},
        )
    });
    let bbox_paths: Option<GeoParquetBboxCovering> = bbox_paths.map(|x| x.into());

    let mut options = GeoParquetReaderOptions::default();

    if let Some(batch_size) = batch_size {
        options = options.with_batch_size(batch_size);
    }
    if let Some(limit) = limit {
        options = options.with_limit(limit);
    }
    if let Some(offset) = offset {
        options = options.with_offset(offset);
    }
    match (bbox, bbox_paths) {
        (Some(bbox), bbox_paths) => {
            options = options.with_bbox(bbox, bbox_paths);
        }
        _ => panic!("Need to pass bbox paths currently with bbox"),
    }

    options = options.with_coord_type(CoordType::Interleaved);

    // TODO: support column projection

    Ok(options)
}
