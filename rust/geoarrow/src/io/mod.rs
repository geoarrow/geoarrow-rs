//! Reader and writer implementations of many common geospatial file formats, including
//! interoperability with the `geozero` crate.

#![allow(missing_docs)] // FIXME

pub mod crs;
#[cfg(feature = "csv")]
pub mod csv;
pub mod display;
#[cfg(feature = "flatgeobuf")]
pub mod flatgeobuf;
#[cfg(feature = "gdal")]
pub mod gdal;
pub mod geojson;
pub mod geojson_lines;
#[cfg(feature = "geos")]
pub mod geos;
pub mod geozero;
pub mod ipc;
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "postgis")]
pub mod postgis;
pub mod shapefile;
mod stream;
pub mod wkb;
pub mod wkt;

pub use stream::RecordBatchReader;
