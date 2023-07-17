//! Contains algorithms that use the rust [`geodesy`] library for
//! geodesic operations.
//!
//! Note that this library does **not** aim to be a PROJ "rewrite in Rust". Consult the [library's
//! documentation][geodesy] for how to construct the projection string to pass into `reproject`.

mod reproject;

pub use geodesy::Direction;
pub use reproject::reproject;
