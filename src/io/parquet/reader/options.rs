use crate::array::CoordType;

/// Options for reading GeoParquet
pub struct GeoParquetReaderOptions {
    /// The number of rows in each batch.
    pub batch_size: usize,

    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    pub bbox: Option<(f64, f64, f64, f64)>,
}

impl Default for GeoParquetReaderOptions {
    fn default() -> Self {
        Self {
            batch_size: 65535,
            coord_type: Default::default(),
            bbox: None,
        }
    }
}
