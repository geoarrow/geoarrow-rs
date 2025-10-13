//! A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification,
//! including algorithms implemented on and returning these GeoArrow arrays.
//!
//! # Reading and writing
//!
//! The [io] module has functions for reading and writing GeoArrow data from a variety of formats.
//! To use most format readers and writers, you must enable their corresponding feature.
//! For example, to convert between [geojson](https://geojson.org/) and GeoArrow, enable the `geozero` feature in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! geoarrow = { version = "*" }
//! ```
//!
//! Then:
//!
//! ```ignore
//! # {
//! use std::{io::Cursor, fs::File};
//!
//! // Reads geojson from a file into a GeoArrow table.
//! let file = File::open("fixtures/roads.geojson").unwrap();
//! let table = geoarrow::io::geojson::read_geojson(file, None).unwrap();
//!
//! // Writes that table to a cursor as JSON, then reads it back into a `serde_json::Value`.
//! let mut cursor = Cursor::new(Vec::new());
//! geoarrow::io::geojson::write_geojson(table, &mut cursor);
//! let value: serde_json::Value = serde_json::from_slice(&cursor.into_inner()).unwrap();
//! # }
//! ```
//!
//! See the [io] module for more information on the available formats and their features.
//!
//! # Constructing
//!
//! You can build GeoArrow arrays all at once from [mod@geo] structures, or anything that implements geometry traits, e.g. [PointTrait](crate::geo_traits::PointTrait).
//! Along with the GeoRust community, **geoarrow-rs** has been prototyping geometry access traits for a standardized way to access coordinate information, regardless of the storage format of the geometries.
//! For now, we vendor an implementation of geo-traits (see [mod@geo_traits]), but this may be upstreamed to georust in the future.
//!
//! ```
//! use geoarrow::array::PointArray;
//! use geoarrow_schema::Dimension;
//!
//! let point = geo::point!(x: 1., y: 2.);
//! let array: PointArray = (vec![point].as_slice(), Dimension::XY).into();
//! ```
//!
//! Or you can use builders, e.g. [PointBuilder](crate::array::PointBuilder):
//!
//! ```
//! use geoarrow::array::PointBuilder;
//! use geoarrow_schema::Dimension;
//!
//! let mut builder = PointBuilder::new(Dimension::XY);
//! builder.push_point(Some(&geo::point!(x: 1., y: 2.)));
//! let array = builder.finish();
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)] // FIXME some modules allow missing docs

pub use trait_::{ArrayBase, NativeArray};

pub mod algorithm;
pub mod array;
pub mod chunked_array;
pub mod datatypes;
pub mod error;
// Long-term we want this to be part of the public API, but not yet stabilized in v0.3.
pub(crate) mod indexed;
pub mod io;
pub mod scalar;
pub mod schema;
pub mod table;
#[cfg(test)]
pub(crate) mod test;
pub mod trait_;
