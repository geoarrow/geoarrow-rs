use geo::coord;
use geoarrow::array::CoordType;
use geoarrow::io::parquet::metadata::GeoParquetBboxCovering;
use geoarrow::io::parquet::GeoParquetReaderOptions;
use pyo3::prelude::*;
use pythonize::depythonize;

use crate::error::PyGeoArrowResult;

pub fn create_options(
    batch_size: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    bbox: Option<[f64; 4]>,
    bbox_paths: Option<Bound<'_, PyAny>>,
) -> PyGeoArrowResult<GeoParquetReaderOptions> {
    let bbox = bbox.map(|item| {
        geo::Rect::new(
            coord! {x: item[0], y: item[1]},
            coord! {x: item[2], y: item[3]},
        )
    });
    let bbox_paths: Option<GeoParquetBboxCovering> =
        bbox_paths.map(|x| depythonize(&x)).transpose()?;

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
