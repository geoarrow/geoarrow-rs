//! The concrete array definitions.
//!
//! All arrays implement the core [GeoArrowArray][crate::GeoArrowArray] trait.

mod coord;
mod geometry;
mod geometrycollection;
mod linestring;
mod mixed;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod rect;
mod wkb;
mod wkt;

pub use coord::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
pub use geometry::GeometryArray;
pub use geometrycollection::GeometryCollectionArray;
pub use linestring::LineStringArray;
pub(crate) use mixed::MixedGeometryArray;
pub use multilinestring::MultiLineStringArray;
pub use multipoint::MultiPointArray;
pub use multipolygon::MultiPolygonArray;
pub use point::PointArray;
pub use polygon::PolygonArray;
pub use rect::RectArray;
pub use wkb::WKBArray;
pub use wkt::WKTArray;

use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Field;

use crate::error::Result;
use crate::{GeoArrowArray, GeoArrowType};

/// Construct a new [GeoArrowArray] from an Arrow [Array] and [Field].
pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;

    let result: Arc<dyn GeoArrowArray> = match GeoArrowType::try_from(field)? {
        Point(_) => Arc::new(PointArray::try_from((array, field))?),
        LineString(_) => Arc::new(LineStringArray::try_from((array, field))?),
        Polygon(_) => Arc::new(PolygonArray::try_from((array, field))?),
        MultiPoint(_) => Arc::new(MultiPointArray::try_from((array, field))?),
        MultiLineString(_) => Arc::new(MultiLineStringArray::try_from((array, field))?),
        MultiPolygon(_) => Arc::new(MultiPolygonArray::try_from((array, field))?),
        GeometryCollection(_) => Arc::new(GeometryCollectionArray::try_from((array, field))?),
        Rect(_) => Arc::new(RectArray::try_from((array, field))?),
        Geometry(_) => Arc::new(GeometryArray::try_from((array, field))?),
        WKB(_) => Arc::new(WKBArray::<i32>::try_from((array, field))?),
        LargeWKB(_) => Arc::new(WKBArray::<i64>::try_from((array, field))?),
        WKT(_) => Arc::new(WKTArray::<i32>::try_from((array, field))?),
        LargeWKT(_) => Arc::new(WKTArray::<i64>::try_from((array, field))?),
    };
    Ok(result)
}
