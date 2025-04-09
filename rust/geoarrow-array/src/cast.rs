//! Helper functions for downcasting [`dyn GeoArrowArray`][GeoArrowArray] to concrete types.

use std::sync::Arc;

use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::trait_::GeoArrowArray;

/// Helpers for downcasting a [`GeoArrowArray`] to a concrete implementation.
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Int32Array, RecordBatch};
/// use arrow_schema::{Schema, Field, DataType, ArrowError};
/// use geo::point;
///
/// use geoarrow_array::array::PointArray;
/// use geoarrow_array::builder::PointBuilder;
/// use geoarrow_array::cast::AsGeoArrowArray;
/// use geoarrow_array::GeoArrowArray;
/// use geo_traits::CoordTrait;
/// use geoarrow_schema::{CoordType, Dimension, PointType};
///
/// let point1 = point!(x: 1., y: 2.);
/// let point2 = point!(x: 3., y: 4.);
/// let point3 = point!(x: 5., y: 6.);
/// let geoms = [point1, point2, point3];
///
/// let geom_type = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
/// let point_array = PointBuilder::from_points(geoms.iter(), geom_type).finish();
///
/// let generic_array: Arc<dyn GeoArrowArray> = Arc::new(point_array.clone());
///
/// let point_array2 = generic_array.as_point();
/// assert_eq!(&point_array, point_array2);
/// ```
pub trait AsGeoArrowArray {
    /// Downcast this to a [`PointArray`] returning `None` if not possible
    fn as_point_opt(&self) -> Option<&PointArray>;

    /// Downcast this to a [`PointArray`] panicking if not possible
    #[inline]
    fn as_point(&self) -> &PointArray {
        self.as_point_opt().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt(&self) -> Option<&LineStringArray>;

    /// Downcast this to a [`LineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string(&self) -> &LineStringArray {
        self.as_line_string_opt().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt(&self) -> Option<&PolygonArray>;

    /// Downcast this to a [`PolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon(&self) -> &PolygonArray {
        self.as_polygon_opt().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray>;

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point(&self) -> &MultiPointArray {
        self.as_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray>;

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string(&self) -> &MultiLineStringArray {
        self.as_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray>;

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon(&self) -> &MultiPolygonArray {
        self.as_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection(&self) -> &GeometryCollectionArray {
        self.as_geometry_collection_opt().unwrap()
    }

    /// Downcast this to a [`RectArray`] returning `None` if not possible
    fn as_rect_opt(&self) -> Option<&RectArray>;

    /// Downcast this to a [`RectArray`] panicking if not possible
    #[inline]
    fn as_rect(&self) -> &RectArray {
        self.as_rect_opt().unwrap()
    }

    /// Downcast this to a [`GeometryArray`] returning `None` if not possible
    fn as_geometry_opt(&self) -> Option<&GeometryArray>;

    /// Downcast this to a [`GeometryArray`] panicking if not possible
    #[inline]
    fn as_geometry(&self) -> &GeometryArray {
        self.as_geometry_opt().unwrap()
    }

    /// Downcast this to a [`WKBArray`] with `O` offsets returning `None` if not possible
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&WKBArray<O>>;

    /// Downcast this to a [`WKBArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_wkb<O: OffsetSizeTrait>(&self) -> &WKBArray<O> {
        self.as_wkb_opt::<O>().unwrap()
    }

    /// Downcast this to a [`WKTArray`] with `O` offsets returning `None` if not possible
    fn as_wkt_opt<O: OffsetSizeTrait>(&self) -> Option<&WKTArray<O>>;

    /// Downcast this to a [`WKTArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_wkt<O: OffsetSizeTrait>(&self) -> &WKTArray<O> {
        self.as_wkt_opt::<O>().unwrap()
    }
}

// `dyn GeoArrowArray + '_` is the same as upstream Arrow
impl AsGeoArrowArray for dyn GeoArrowArray + '_ {
    #[inline]
    fn as_point_opt(&self) -> Option<&PointArray> {
        self.as_any().downcast_ref::<PointArray>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&LineStringArray> {
        self.as_any().downcast_ref::<LineStringArray>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&PolygonArray> {
        self.as_any().downcast_ref::<PolygonArray>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray> {
        self.as_any().downcast_ref::<MultiPointArray>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray> {
        self.as_any().downcast_ref::<MultiLineStringArray>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray> {
        self.as_any().downcast_ref::<MultiPolygonArray>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray> {
        self.as_any().downcast_ref::<GeometryCollectionArray>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&RectArray> {
        self.as_any().downcast_ref::<RectArray>()
    }

    #[inline]
    fn as_geometry_opt(&self) -> Option<&GeometryArray> {
        self.as_any().downcast_ref::<GeometryArray>()
    }

    #[inline]
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&WKBArray<O>> {
        self.as_any().downcast_ref::<WKBArray<O>>()
    }

    #[inline]
    fn as_wkt_opt<O: OffsetSizeTrait>(&self) -> Option<&WKTArray<O>> {
        self.as_any().downcast_ref::<WKTArray<O>>()
    }
}

impl AsGeoArrowArray for Arc<dyn GeoArrowArray> {
    #[inline]
    fn as_point_opt(&self) -> Option<&PointArray> {
        self.as_any().downcast_ref::<PointArray>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&LineStringArray> {
        self.as_any().downcast_ref::<LineStringArray>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&PolygonArray> {
        self.as_any().downcast_ref::<PolygonArray>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray> {
        self.as_any().downcast_ref::<MultiPointArray>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray> {
        self.as_any().downcast_ref::<MultiLineStringArray>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray> {
        self.as_any().downcast_ref::<MultiPolygonArray>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray> {
        self.as_any().downcast_ref::<GeometryCollectionArray>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&RectArray> {
        self.as_any().downcast_ref::<RectArray>()
    }

    #[inline]
    fn as_geometry_opt(&self) -> Option<&GeometryArray> {
        self.as_any().downcast_ref::<GeometryArray>()
    }

    #[inline]
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&WKBArray<O>> {
        self.as_any().downcast_ref::<WKBArray<O>>()
    }

    #[inline]
    fn as_wkt_opt<O: OffsetSizeTrait>(&self) -> Option<&WKTArray<O>> {
        self.as_any().downcast_ref::<WKTArray<O>>()
    }
}
