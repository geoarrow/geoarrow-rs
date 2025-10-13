//! A complete, safe, native Rust implementation of [GeoArrow](https://geoarrow.org/), which adds
//! geospatial support to the [Apache Arrow](https://arrow.apache.org) tabular in-memory data
//! format.
//!
//! As of version 0.4, the `geoarrow` crate was refactored to a monorepo of smaller crates, each
//! with a more well-defined scope. Users may want to depend on the subcrates manually:
//!
//! - [`geoarrow_array`]: GeoArrow array definitions.
//! - [`geoarrow_cast`](https://docs.rs/geoarrow-cast/latest/geoarrow_cast/): Functions for converting from one GeoArrow geometry type to another.
//! - [`geoarrow_schema`]: GeoArrow geometry type and metadata definitions.
//!
//! This crate is an "amalgam" crate, which just re-exports types from the underlying sub-crates.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]
pub mod array;
pub mod datatypes;
pub mod error;
