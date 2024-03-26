#[derive(Copy, Clone)]
pub enum GeoParquetWriterEncoding {
    WKB,
    Native,
}

/// Options for writing GeoParquet
pub struct GeoParquetWriterOptions {
    encoding: GeoParquetWriterEncoding,
}

impl Default for GeoParquetWriterOptions {
    fn default() -> Self {
        Self {
            encoding: GeoParquetWriterEncoding::WKB,
        }
    }
}
