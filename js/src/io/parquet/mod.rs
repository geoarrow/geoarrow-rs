#[cfg(feature = "io_parquet_async")]
pub mod r#async;
#[cfg(feature = "io_parquet_async")]
pub mod async_file_reader;
pub mod sync;

#[cfg(feature = "io_parquet_async")]
pub use r#async::{ParquetDataset, ParquetFile};
pub use sync::read_geoparquet;
