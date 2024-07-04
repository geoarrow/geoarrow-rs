use geo::coord;
use geoarrow::array::CoordType;
use geoarrow::io::parquet::GeoParquetReaderOptions;
use pyo3::prelude::*;

#[derive(FromPyObject)]
pub struct GeoParquetBboxPaths {
    #[pyo3(item)]
    minx_path: Vec<String>,
    #[pyo3(item)]
    miny_path: Vec<String>,
    #[pyo3(item)]
    maxx_path: Vec<String>,
    #[pyo3(item)]
    maxy_path: Vec<String>,
}

impl From<GeoParquetBboxPaths> for geoarrow::io::parquet::ParquetBboxPaths {
    fn from(value: GeoParquetBboxPaths) -> Self {
        Self {
            minx_path: value.minx_path,
            miny_path: value.miny_path,
            maxx_path: value.maxx_path,
            maxy_path: value.maxy_path,
        }
    }
}

pub fn create_options(
    batch_size: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    bbox: Option<[f64; 4]>,
    bbox_paths: Option<GeoParquetBboxPaths>,
) -> GeoParquetReaderOptions {
    let bbox = bbox.map(|item| {
        geo::Rect::new(
            coord! {x: item[0], y: item[1]},
            coord! {x: item[2], y: item[3]},
        )
    });
    let bbox_paths = bbox_paths.map(geoarrow::io::parquet::ParquetBboxPaths::from);

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
        (Some(bbox), Some(bbox_paths)) => {
            options = options.with_bbox(bbox, bbox_paths);
        }
        _ => panic!("Need to pass bbox paths currently with bbox"),
    }

    options = options.with_coord_type(CoordType::Interleaved);

    // TODO: support column projection

    options
}
