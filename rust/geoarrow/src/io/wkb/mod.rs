//! Read and write geometries encoded as [Well-Known Binary](https://libgeos.org/specifications/wkb/).
//!
//! This wraps the [wkb] crate. As such, it currently supports reading the ISO and extended (EWKB)
//! variants of WKB. Currently, it always writes the ISO WKB variant.

mod api;
pub(crate) mod writer;

pub use api::{FromWKB, ToWKB, from_wkb, to_wkb};
