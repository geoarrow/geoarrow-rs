use geoarrow_schema::crs::CrsTransform;

/// Allowed encodings when writing to GeoParquet
#[derive(Copy, Clone, Default)]
#[allow(clippy::upper_case_acronyms)]
pub enum GeoParquetWriterEncoding {
    /// Well-known binary geometry encoding
    ///
    /// This is the only encoding supported in GeoParquet version 1.0, so if you wish to maintain
    /// compatibility with that version, you must choose WKB.
    #[default]
    WKB,

    /// GeoArrow-native encoding. This is supported as of GeoParquet version 1.1.
    GeoArrow,
}

/// Options for writing GeoParquet
#[derive(Default)]
pub struct GeoParquetWriterOptions {
    /// Set the type of encoding to use for writing to GeoParquet.
    pub encoding: GeoParquetWriterEncoding,

    /// Set the primary geometry column name.
    pub primary_column: Option<String>,

    /// A transformer for converting CRS from the GeoArrow representation to PROJJSON.
    pub crs_transform: Option<Box<dyn CrsTransform>>,
}
