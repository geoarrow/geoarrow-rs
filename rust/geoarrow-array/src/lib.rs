//! Implementations of immutable GeoArrow arrays plus builders to more easily create arrays.
//!
//! There are three primary types of structs in this module: arrays, builders, and capacity
//! counters.
//!
//! ## Arrays
//!
//! Arrays
//!
//! These arrays implement the binary layout defined in the [GeoArrow specification](https://github.com/geoarrow/geoarrow).
//!
//!
//!
//! These include:
//!
//! - [`PointArray`]
//! - [`LineStringArray`]
//! - [`PolygonArray`]
//! - [`MultiPointArray`]
//! - [`MultiLineStringArray`]
//! - [`MultiPolygonArray`]
//! - [`GeometryArray`]
//! - [`GeometryCollectionArray`]
//! - [`RectArray`]
//!
//! ## Builders
//!
//! Builders are designed to make it easier
//!
//! There's a builder for each of the above array types:
//!
//!
//! - [`PointBuilder`]
//! - [`LineStringBuilder`]
//! - [`PolygonBuilder`]
//! - [`MultiPointBuilder`]
//! - [`MultiLineStringBuilder`]
//! - [`MultiPolygonBuilder`]
//! - [`GeometryBuilder`]
//! - [`GeometryCollectionBuilder`]
//! - [`RectBuilder`]
//!
//! Once you've finished adding geometries to a builder, it's `O(1)` to convert a builder to an
//! array, by calling `finish()`.
//!
//! ## Capacity Counters
//!
//! Underlying the builders are growable `Vec`s. E.g. you can think of a `PointBuilder` as a buffer of `x` coordinates and a buffer of `y` coordinates.
//!
//! The fastest and most memory-efficient way to construct an array from a set of known geometries
//! is to make a first pass over these geometries to count exactly how big each part of the Arrow
//! array must be, allocate _once_ for exactly what you need, and then fill those buffers in a
//! second pass.
//!

pub mod array;
pub mod builder;
pub mod capacity;
pub mod cast;
mod datatypes;
mod eq;
pub mod error;
pub mod scalar;
mod trait_;
pub(crate) mod util;

pub use datatypes::{AnyType, NativeType, SerializedType};
pub use trait_::{ArrayAccessor, IntoArrow};

#[cfg(test)]
pub(crate) mod test;
