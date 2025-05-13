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
mod wkb_view;
mod wkt;
mod wkt_view;

pub use coord::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
pub(crate) use geometry::DimensionIndex;
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
pub use wkb::WkbArray;
pub use wkb_view::WkbViewArray;
pub use wkt::WktArray;
pub use wkt_view::WktViewArray;

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
        Wkb(_) => Arc::new(WkbArray::<i32>::try_from((array, field))?),
        LargeWkb(_) => Arc::new(WkbArray::<i64>::try_from((array, field))?),
        WkbView(_) => Arc::new(WkbViewArray::try_from((array, field))?),
        Wkt(_) => Arc::new(WktArray::<i32>::try_from((array, field))?),
        LargeWkt(_) => Arc::new(WktArray::<i64>::try_from((array, field))?),
        WktView(_) => Arc::new(WktViewArray::try_from((array, field))?),
    };
    Ok(result)
}

/// A trait for GeoArrow arrays that can hold WKB data.
///
/// Currently three types are supported:
///
/// - [`WkbArray<i32>`]
/// - [`WkbArray<i64>`]
/// - [`WkbViewArray`]
///
/// This trait helps to abstract over the different types of WKB arrays so that we don’t need to
/// duplicate the implementation for each type.
///
/// This is modeled after the upstream [`BinaryArrayType`][arrow_array::array::BinaryArrayType]
/// trait.
pub trait WkbArrayType<'a>:
    Sized + crate::ArrayAccessor<'a, Item = ::wkb::reader::Wkb<'a>>
{
}

impl<'a> WkbArrayType<'a> for WkbArray<i32> {}
impl<'a> WkbArrayType<'a> for WkbArray<i64> {}
impl<'a> WkbArrayType<'a> for WkbViewArray {}

/// A trait for GeoArrow arrays that can hold WKT data.
///
/// Currently three types are supported:
///
/// - [`WktArray<i32>`]
/// - [`WktArray<i64>`]
/// - [`WktViewArray`]
///
/// This trait helps to abstract over the different types of WKT arrays so that we don’t need to
/// duplicate the implementation for each type.
///
/// This is modeled after the upstream [`StringArrayType`][arrow_array::array::StringArrayType]
/// trait.
pub trait WktArrayType: Sized + for<'a> crate::ArrayAccessor<'a, Item = ::wkt::Wkt> {}

impl WktArrayType for WktArray<i32> {}
impl WktArrayType for WktArray<i64> {}
impl WktArrayType for WktViewArray {}
