//! `geoarrow`: A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow)
//! specification.

pub use binary::{MutableWKBArray, WKBArray, WKB};
pub use enum_::GeometryArray;
pub use linestring::{LineString, LineStringArray, MutableLineStringArray};
pub use multilinestring::{MultiLineString, MultiLineStringArray, MutableMultiLineStringArray};
pub use multipoint::{MultiPoint, MultiPointArray, MutableMultiPointArray};
pub use multipolygon::{MultiPolygon, MultiPolygonArray, MutableMultiPolygonArray};
pub use point::{MutablePointArray, Point, PointArray};
pub use polygon::{MutablePolygonArray, Polygon, PolygonArray};
pub use trait_::GeometryArrayTrait;

pub mod algorithm;
pub mod binary;
pub mod enum_;
pub mod error;
pub mod geo_traits;
pub mod linestring;
pub mod multilinestring;
pub mod multipoint;
pub mod multipolygon;
pub mod point;
pub mod polygon;
pub mod trait_;
