#![doc = include_str!("README.md")]

#[cfg(feature = "async")]
mod r#async;
mod geo_ext;
mod metadata;
mod parse;
mod spatial_filter;
mod sync;

#[cfg(feature = "async")]
pub use r#async::GeoParquetRecordBatchStream;
pub use geo_ext::GeoParquetReaderBuilder;
pub use metadata::{GeoParquetDatasetMetadata, GeoParquetReaderMetadata};
pub use parse::infer_geoarrow_schema;
pub use sync::GeoParquetRecordBatchReader;
