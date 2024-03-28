use geo::Rect;

use crate::array::CoordType;
use crate::io::parquet::reader::spatial_filter::ParquetBboxStatistics;

/// Options for reading GeoParquet
pub struct GeoParquetReaderOptions {
    /// The number of rows in each batch.
    pub batch_size: usize,

    /// The GeoArrow coordinate type to use in the geometry arrays.
    ///
    /// Note that for now this is only used when parsing from WKB-encoded geometries.
    pub coord_type: CoordType,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    pub bbox: Option<Rect>,

    /// The paths in the Parquet schema to the bounding box columns. This will not be necessary as
    /// of GeoParquet 1.1.
    pub bbox_cols: Option<ParquetBboxStatistics>,
}

impl Default for GeoParquetReaderOptions {
    fn default() -> Self {
        Self {
            batch_size: 65535,
            coord_type: Default::default(),
            bbox: None,
            bbox_cols: None,
        }
    }
}
