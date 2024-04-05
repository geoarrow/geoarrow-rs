#[cfg(feature = "parquet_async")]
mod r#async;
mod options;
// mod parse;
mod spatial_filter;
mod sync;

pub use options::ParquetReaderOptions;
#[cfg(feature = "parquet_async")]
pub use r#async::{read_geoparquet_async, ParquetDataset, ParquetFile};
pub use spatial_filter::ParquetBboxPaths;
pub use sync::read_geoparquet;
