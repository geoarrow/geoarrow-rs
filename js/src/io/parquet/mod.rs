#[cfg(feature = "io_parquet_async")]
pub mod r#async;
pub mod sync;

#[cfg(feature = "io_parquet_async")]
pub use r#async::{ParquetDataset, ParquetFile};
pub use sync::read_geoparquet;
