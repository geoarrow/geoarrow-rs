#[cfg(feature = "parquet_async")]
mod r#async;
mod options;
// mod parse;
mod sync;

pub use options::GeoParquetReaderOptions;
#[cfg(feature = "parquet_async")]
pub use r#async::{read_geoparquet_async, ParquetDataset, ParquetFile, ParquetReaderOptions};
pub use sync::read_geoparquet;
