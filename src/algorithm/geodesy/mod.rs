//! Bindings to the [`geodesy`] crate for coordinate reprojection.
//!
//! Note that this library does **not** aim to be a full PROJ "rewrite in Rust". Consult the
//! [library's documentation][geodesy] for how to construct the projection string to pass into
//! `reproject`.

mod reproject;

pub use geodesy::Direction;
pub use reproject::reproject;
