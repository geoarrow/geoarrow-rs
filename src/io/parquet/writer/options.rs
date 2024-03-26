use parquet::file::properties::WriterProperties;

#[derive(Copy, Clone)]
#[allow(clippy::upper_case_acronyms)]
pub enum GeoParquetWriterEncoding {
    WKB,
    Native,
}

/// Options for writing GeoParquet
pub struct GeoParquetWriterOptions {
    pub encoding: GeoParquetWriterEncoding,
    pub writer_properties: Option<WriterProperties>,
}

impl Default for GeoParquetWriterOptions {
    fn default() -> Self {
        Self {
            encoding: GeoParquetWriterEncoding::WKB,
            writer_properties: None,
        }
    }
}
