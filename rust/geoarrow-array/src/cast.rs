use crate::array::*;
use crate::trait_::GeoArrowArray;

/// Helpers for downcasting a [`GeoArrowArray`] to a concrete implementation.
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

    /// Downcast this to a [`WKBArray`] with `i32` offsets returning `None` if not possible
    fn as_wkb_opt(&self) -> Option<&WKBArray<i32>>;

    /// Downcast this to a [`WKBArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_wkb(&self) -> &WKBArray<i32> {
        self.as_wkb_opt().unwrap()
    }

    /// Downcast this to a [`WKBArray`] with `i64` offsets returning `None` if not possible
    fn as_large_wkb_opt(&self) -> Option<&WKBArray<i64>>;

    /// Downcast this to a [`WKBArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_wkb(&self) -> &WKBArray<i64> {
        self.as_large_wkb_opt().unwrap()
    }
}

impl AsGeoArrowArray for &dyn GeoArrowArray {
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
    fn as_wkb_opt(&self) -> Option<&WKBArray<i32>> {
        self.as_any().downcast_ref::<WKBArray<i32>>()
    }

    #[inline]
    fn as_large_wkb_opt(&self) -> Option<&WKBArray<i64>> {
        self.as_any().downcast_ref::<WKBArray<i64>>()
    }
}
