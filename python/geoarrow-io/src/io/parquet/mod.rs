#[cfg(feature = "async")]
mod r#async;
mod sync;

pub mod options;

#[cfg(feature = "async")]
pub use r#async::{read_parquet_async, ParquetDataset, ParquetFile};
pub use sync::{read_parquet, write_parquet, ParquetWriter};
