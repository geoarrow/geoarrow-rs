pub mod sync;

pub use sync::{read_geoparquet, write_geoparquet};

#[cfg(feature = "io_parquet_async")]
pub mod r#async;

#[cfg(feature = "io_parquet_async")]
pub use r#async::{ParquetDataset, ParquetFile};
