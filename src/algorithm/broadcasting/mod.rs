//! Structs intended to help with
//! ["broadcasting"](https://numpy.org/doc/stable/user/basics.broadcasting.html) for applying
//! either a scalar or an array as an argument into an operation.
//!
//! **NOTE! This approach to broadcasting was an early prototype but is likely to be removed in
//! the future.**
//!
//! Many algorithms, such as [`Translate`][crate::algorithm::geo::Translate], accept either a
//! scalar or an array as input. If you pass in a scalar, every geometry in the geometry array will
//! be moved by the same amount, whereas if you pass in an array of values, every geometry will
//! have a different argument applied.
//!
//! The objects in this module are enums with two variants: `Scalar` and `Array`. Create an object
//! with the variant desired for your operation.
//!
//! For simplicity, the `Scalar` variants of the geometry broadcasting enums, such as
//! [`BroadcastablePoint`] accept a [`geo`] object, not a GeoArrow scalar object.

// mod geometry;
// mod linestring;
// mod multilinestring;
// mod multipoint;
// mod multipolygon;
// mod point;
// mod polygon;
mod primitive;
mod vec;

// pub use geometry::BroadcastableGeometry;
// pub use linestring::BroadcastableLineString;
// pub use multilinestring::BroadcastableMultiLineString;
// pub use multipoint::BroadcastableMultiPoint;
// pub use multipolygon::BroadcastableMultiPolygon;
// pub use point::BroadcastablePoint;
// pub use polygon::BroadcastablePolygon;
pub use primitive::BroadcastablePrimitive;
pub use vec::BroadcastableVec;
