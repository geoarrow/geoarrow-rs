#[cfg(feature = "async")]
mod r#async;
mod sync;

pub mod options;

#[cfg(feature = "async")]
pub use r#async::{ParquetDataset, ParquetFile, read_parquet_async};
pub use sync::{ParquetWriter, read_parquet, write_parquet};
