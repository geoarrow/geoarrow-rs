#![doc = include_str!("README.md")]

mod encode;
mod metadata;
mod options;

pub use encode::GeoParquetRecordBatchEncoder;
pub use options::{
    GeoParquetWriterEncoding, GeoParquetWriterOptions, GeoParquetWriterOptionsBuilder,
};
