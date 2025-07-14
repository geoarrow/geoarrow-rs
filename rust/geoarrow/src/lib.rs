//! A complete, safe, native Rust implementation of [GeoArrow](https://geoarrow.org/), which adds
//! geospatial support to the [Apache Arrow](https://arrow.apache.org) tabular in-memory data
//! format.
//!
//! As of version 0.4, the `geoarrow` crate was refactored to a monorepo of smaller crates, each
//! with a more well-defined scope. Users may want to depend on the subcrates manually:
//!
//! - [`geoarrow_array`]: GeoArrow array definitions.
//! - [`geoarrow_cast`]: Functions for converting from one GeoArrow geometry type to another.
//! - [`geoarrow_schema`]: GeoArrow geometry type and metadata definitions.
//!
//! This crate is an "amalgam" crate, which just re-exports types from the underlying sub-crates.

pub mod array;
pub mod datatypes;
pub mod error;
