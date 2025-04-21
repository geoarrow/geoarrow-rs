#![doc = include_str!("README.md")]

#[cfg(feature = "async")]
mod r#async;
mod builder;
mod metadata;
mod options;
mod parse;
mod spatial_filter;

#[cfg(feature = "async")]
pub use r#async::{GeoParquetRecordBatchStream, GeoParquetRecordBatchStreamBuilder};
pub use builder::{GeoParquetRecordBatchReader, GeoParquetRecordBatchReaderBuilder};
pub use metadata::{GeoParquetDatasetMetadata, GeoParquetReaderMetadata};
pub use options::GeoParquetReaderOptions;
