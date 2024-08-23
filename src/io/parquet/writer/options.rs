use parquet::file::properties::WriterProperties;

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
    Native,
}

/// Options for writing GeoParquet
#[derive(Clone, Default)]
pub struct GeoParquetWriterOptions {
    /// Set the type of encoding to use for writing to GeoParquet.
    pub encoding: GeoParquetWriterEncoding,

    /// The parquet [WriterProperties] to use for writing to file
    pub writer_properties: Option<WriterProperties>,
}
