#![doc = include_str!("README.md")]

#[cfg(feature = "async")]
mod r#async;
mod encode;
mod metadata;
mod options;
mod sync;

#[cfg(feature = "async")]
pub use r#async::{GeoParquetWriterAsync, write_geoparquet_async};
pub use options::{GeoParquetWriterEncoding, GeoParquetWriterOptions};
pub use sync::{GeoParquetWriter, write_geoparquet};
