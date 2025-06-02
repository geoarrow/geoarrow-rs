#[cfg(feature = "async")]
mod r#async;
mod sync;

pub mod options;

#[cfg(feature = "async")]
pub use r#async::{GeoParquetDataset, GeoParquetFile, read_parquet_async};
pub use sync::{PyGeoParquetWriter, read_parquet, write_parquet};
