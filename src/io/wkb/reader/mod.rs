//! Parse WKB arrays
//!
//! Each of the data structures in this module is intended to mirror the [WKB
//! spec](https://portal.ogc.org/files/?artifact_id=25355). Crucially each of these data structures
//! implement geometry access traits for interoperability and each of these data structures should
//! be O(1) access to any given coordinate.

mod coord;
mod geometry;
mod geometry_collection;
mod linearring;
mod linestring;
mod maybe_multi_line_string;
mod maybe_multi_point;
mod maybe_multipolygon;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod rect;
pub(crate) mod r#type;

pub use geometry::{Endianness, WKBGeometry};
pub use linestring::WKBLineString;
pub use maybe_multi_line_string::WKBMaybeMultiLineString;
pub use maybe_multi_point::WKBMaybeMultiPoint;
pub use maybe_multipolygon::WKBMaybeMultiPolygon;
pub use multilinestring::WKBMultiLineString;
pub use multipoint::WKBMultiPoint;
pub use multipolygon::WKBMultiPolygon;
pub use point::WKBPoint;
pub use polygon::WKBPolygon;
pub use r#type::WKBGeometryType;
