use geo::coord;
use geoarrow::array::CoordType;
use geoarrow::io::parquet::GeoParquetReaderOptions;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsGeoParquetBboxPaths {
    pub xmin: Vec<String>,
    pub ymin: Vec<String>,
    pub xmax: Vec<String>,
    pub ymax: Vec<String>,
}

impl From<JsGeoParquetBboxPaths> for geoarrow::io::parquet::metadata::GeoParquetBboxCovering {
    fn from(value: JsGeoParquetBboxPaths) -> Self {
        Self {
            xmin: value.xmin,
            ymin: value.ymin,
            zmin: None,
            xmax: value.xmax,
            ymax: value.ymax,
            zmax: None,
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct JsParquetReaderOptions {
    /// The number of rows in each batch. If not provided, the upstream [parquet] default is 1024.
    pub batch_size: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_limit]
    pub limit: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_offset]
    pub offset: Option<usize>,

    pub bbox: Option<Vec<f64>>,

    pub bbox_paths: Option<JsGeoParquetBboxPaths>,
}

impl From<JsParquetReaderOptions> for geoarrow::io::parquet::GeoParquetReaderOptions {
    fn from(value: JsParquetReaderOptions) -> Self {
        let bbox = value.bbox.map(|item| {
            geo::Rect::new(
                coord! {x: item[0], y: item[1]},
                coord! {x: item[2], y: item[3]},
            )
        });

        let mut options = GeoParquetReaderOptions::default();

        if let Some(batch_size) = value.batch_size {
            options = options.with_batch_size(batch_size);
        }
        if let Some(limit) = value.limit {
            options = options.with_limit(limit);
        }
        if let Some(offset) = value.offset {
            options = options.with_offset(offset);
        }
        match (bbox, value.bbox_paths) {
            (Some(bbox), bbox_paths) => {
                options = options.with_bbox(bbox, bbox_paths.map(|x| x.into()));
            }
            _ => panic!("Need to pass bbox paths currently with bbox"),
        }

        options.with_coord_type(CoordType::Interleaved)
    }
}
