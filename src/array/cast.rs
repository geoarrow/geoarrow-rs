use crate::array::*;
use crate::chunked_array::*;

/// Helpers for downcasting a [`GeometryArrayTrait`] to a concrete implementation.
pub trait AsGeometryArray {
    /// Downcast this to a [`PointArray`] returning `None` if not possible
    fn as_point_opt(&self) -> Option<&PointArray<2>>;

    /// Downcast this to a [`PointArray`] panicking if not possible
    #[inline]
    fn as_point(&self) -> &PointArray<2> {
        self.as_point_opt().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt(&self) -> Option<&LineStringArray<i32, 2>>;

    /// Downcast this to a [`LineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string(&self) -> &LineStringArray<i32, 2> {
        self.as_line_string_opt().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `i64` offsets returning `None` if not possible
    fn as_large_line_string_opt(&self) -> Option<&LineStringArray<i64, 2>>;

    /// Downcast this to a [`LineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_line_string(&self) -> &LineStringArray<i64, 2> {
        self.as_large_line_string_opt().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt(&self) -> Option<&PolygonArray<i32, 2>>;

    /// Downcast this to a [`PolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon(&self) -> &PolygonArray<i32, 2> {
        self.as_polygon_opt().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i64` offsets returning `None` if not possible
    fn as_large_polygon_opt(&self) -> Option<&PolygonArray<i64, 2>>;

    /// Downcast this to a [`PolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_polygon(&self) -> &PolygonArray<i64, 2> {
        self.as_large_polygon_opt().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray<i32, 2>>;

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point(&self) -> &MultiPointArray<i32, 2> {
        self.as_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i64` offsets returning `None` if not possible
    fn as_large_multi_point_opt(&self) -> Option<&MultiPointArray<i64, 2>>;

    /// Downcast this to a [`MultiPointArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_point(&self) -> &MultiPointArray<i64, 2> {
        self.as_large_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray<i32, 2>>;

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string(&self) -> &MultiLineStringArray<i32, 2> {
        self.as_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_line_string_opt(&self) -> Option<&MultiLineStringArray<i64, 2>>;

    /// Downcast this to a [`MultiLineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_line_string(&self) -> &MultiLineStringArray<i64, 2> {
        self.as_large_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray<i32, 2>>;

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon(&self) -> &MultiPolygonArray<i32, 2> {
        self.as_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_polygon_opt(&self) -> Option<&MultiPolygonArray<i64, 2>>;

    /// Downcast this to a [`MultiPolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_polygon(&self) -> &MultiPolygonArray<i64, 2> {
        self.as_large_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`MixedGeometryArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_mixed_opt(&self) -> Option<&MixedGeometryArray<i32, 2>>;

    /// Downcast this to a [`MixedGeometryArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_mixed(&self) -> &MixedGeometryArray<i32, 2> {
        self.as_mixed_opt().unwrap()
    }

    /// Downcast this to a [`MixedGeometryArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_mixed_opt(&self) -> Option<&MixedGeometryArray<i64, 2>>;

    /// Downcast this to a [`MixedGeometryArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_mixed(&self) -> &MixedGeometryArray<i64, 2> {
        self.as_large_mixed_opt().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray<i32, 2>>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection(&self) -> &GeometryCollectionArray<i32, 2> {
        self.as_geometry_collection_opt().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray<i64, 2>>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_geometry_collection(&self) -> &GeometryCollectionArray<i64, 2> {
        self.as_large_geometry_collection_opt().unwrap()
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

    /// Downcast this to a [`RectArray`] returning `None` if not possible
    fn as_rect_opt(&self) -> Option<&RectArray>;

    /// Downcast this to a [`RectArray`] panicking if not possible
    #[inline]
    fn as_rect(&self) -> &RectArray {
        self.as_rect_opt().unwrap()
    }
}

impl AsGeometryArray for &dyn GeometryArrayTrait {
    #[inline]
    fn as_point_opt(&self) -> Option<&PointArray<2>> {
        self.as_any().downcast_ref::<PointArray<2>>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&LineStringArray<i32, 2>> {
        self.as_any().downcast_ref::<LineStringArray<i32, 2>>()
    }

    #[inline]
    fn as_large_line_string_opt(&self) -> Option<&LineStringArray<i64, 2>> {
        self.as_any().downcast_ref::<LineStringArray<i64, 2>>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&PolygonArray<i32, 2>> {
        self.as_any().downcast_ref::<PolygonArray<i32, 2>>()
    }

    #[inline]
    fn as_large_polygon_opt(&self) -> Option<&PolygonArray<i64, 2>> {
        self.as_any().downcast_ref::<PolygonArray<i64, 2>>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray<i32, 2>> {
        self.as_any().downcast_ref::<MultiPointArray<i32, 2>>()
    }

    #[inline]
    fn as_large_multi_point_opt(&self) -> Option<&MultiPointArray<i64, 2>> {
        self.as_any().downcast_ref::<MultiPointArray<i64, 2>>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray<i32, 2>> {
        self.as_any().downcast_ref::<MultiLineStringArray<i32, 2>>()
    }

    #[inline]
    fn as_large_multi_line_string_opt(&self) -> Option<&MultiLineStringArray<i64, 2>> {
        self.as_any().downcast_ref::<MultiLineStringArray<i64, 2>>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray<i32, 2>> {
        self.as_any().downcast_ref::<MultiPolygonArray<i32, 2>>()
    }

    #[inline]
    fn as_large_multi_polygon_opt(&self) -> Option<&MultiPolygonArray<i64, 2>> {
        self.as_any().downcast_ref::<MultiPolygonArray<i64, 2>>()
    }

    #[inline]
    fn as_mixed_opt(&self) -> Option<&MixedGeometryArray<i32, 2>> {
        self.as_any().downcast_ref::<MixedGeometryArray<i32, 2>>()
    }

    #[inline]
    fn as_large_mixed_opt(&self) -> Option<&MixedGeometryArray<i64, 2>> {
        self.as_any().downcast_ref::<MixedGeometryArray<i64, 2>>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<GeometryCollectionArray<i32, 2>>()
    }

    #[inline]
    fn as_large_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<GeometryCollectionArray<i64, 2>>()
    }

    #[inline]
    fn as_wkb_opt(&self) -> Option<&WKBArray<i32>> {
        self.as_any().downcast_ref::<WKBArray<i32>>()
    }

    #[inline]
    fn as_large_wkb_opt(&self) -> Option<&WKBArray<i64>> {
        self.as_any().downcast_ref::<WKBArray<i64>>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&RectArray> {
        self.as_any().downcast_ref::<RectArray>()
    }
}

/// Helpers for downcasting a [`ChunkedGeometryArrayTrait`] to a concrete implementation.
pub trait AsChunkedGeometryArray {
    /// Downcast this to a [`ChunkedPointArray`] returning `None` if not possible
    fn as_point_opt(&self) -> Option<&ChunkedPointArray<2>>;

    /// Downcast this to a [`ChunkedPointArray`] panicking if not possible
    #[inline]
    fn as_point(&self) -> &ChunkedPointArray<2> {
        self.as_point_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt(&self) -> Option<&ChunkedLineStringArray<i32, 2>>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string(&self) -> &ChunkedLineStringArray<i32, 2> {
        self.as_line_string_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedLineStringArray`] with `i64` offsets returning `None` if not possible
    fn as_large_line_string_opt(&self) -> Option<&ChunkedLineStringArray<i64, 2>>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_line_string(&self) -> &ChunkedLineStringArray<i64, 2> {
        self.as_large_line_string_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt(&self) -> Option<&ChunkedPolygonArray<i32, 2>>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon(&self) -> &ChunkedPolygonArray<i32, 2> {
        self.as_polygon_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `i64` offsets returning `None` if not possible
    fn as_large_polygon_opt(&self) -> Option<&ChunkedPolygonArray<i64, 2>>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_polygon(&self) -> &ChunkedPolygonArray<i64, 2> {
        self.as_large_polygon_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt(&self) -> Option<&ChunkedMultiPointArray<i32, 2>>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point(&self) -> &ChunkedMultiPointArray<i32, 2> {
        self.as_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i64` offsets returning `None` if not possible
    fn as_large_multi_point_opt(&self) -> Option<&ChunkedMultiPointArray<i64, 2>>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_point(&self) -> &ChunkedMultiPointArray<i64, 2> {
        self.as_large_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt(&self) -> Option<&ChunkedMultiLineStringArray<i32, 2>>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string(&self) -> &ChunkedMultiLineStringArray<i32, 2> {
        self.as_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_line_string_opt(&self) -> Option<&ChunkedMultiLineStringArray<i64, 2>>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_line_string(&self) -> &ChunkedMultiLineStringArray<i64, 2> {
        self.as_large_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt(&self) -> Option<&ChunkedMultiPolygonArray<i32, 2>>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon(&self) -> &ChunkedMultiPolygonArray<i32, 2> {
        self.as_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_polygon_opt(&self) -> Option<&ChunkedMultiPolygonArray<i64, 2>>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_polygon(&self) -> &ChunkedMultiPolygonArray<i64, 2> {
        self.as_large_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_mixed_opt(&self) -> Option<&ChunkedMixedGeometryArray<i32, 2>>;

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_mixed(&self) -> &ChunkedMixedGeometryArray<i32, 2> {
        self.as_mixed_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_mixed_opt(&self) -> Option<&ChunkedMixedGeometryArray<i64, 2>>;

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_mixed(&self) -> &ChunkedMixedGeometryArray<i64, 2> {
        self.as_large_mixed_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt(&self) -> Option<&ChunkedGeometryCollectionArray<i32, 2>>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection(&self) -> &ChunkedGeometryCollectionArray<i32, 2> {
        self.as_geometry_collection_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_geometry_collection_opt(&self) -> Option<&ChunkedGeometryCollectionArray<i64, 2>>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_geometry_collection(&self) -> &ChunkedGeometryCollectionArray<i64, 2> {
        self.as_large_geometry_collection_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedWKBArray`] with `i32` offsets returning `None` if not possible
    fn as_wkb_opt(&self) -> Option<&ChunkedWKBArray<i32>>;

    /// Downcast this to a [`ChunkedWKBArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_wkb(&self) -> &ChunkedWKBArray<i32> {
        self.as_wkb_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedWKBArray`] with `i64` offsets returning `None` if not possible
    fn as_large_wkb_opt(&self) -> Option<&ChunkedWKBArray<i64>>;

    /// Downcast this to a [`ChunkedWKBArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_wkb(&self) -> &ChunkedWKBArray<i64> {
        self.as_large_wkb_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedRectArray`] returning `None` if not possible
    fn as_rect_opt(&self) -> Option<&ChunkedRectArray>;

    /// Downcast this to a [`ChunkedRectArray`] panicking if not possible
    #[inline]
    fn as_rect(&self) -> &ChunkedRectArray {
        self.as_rect_opt().unwrap()
    }
}

impl AsChunkedGeometryArray for &dyn ChunkedGeometryArrayTrait {
    #[inline]
    fn as_point_opt(&self) -> Option<&ChunkedPointArray<2>> {
        self.as_any().downcast_ref::<ChunkedPointArray<2>>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&ChunkedLineStringArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedLineStringArray<i32, 2>>()
    }

    #[inline]
    fn as_large_line_string_opt(&self) -> Option<&ChunkedLineStringArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedLineStringArray<i64, 2>>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&ChunkedPolygonArray<i32, 2>> {
        self.as_any().downcast_ref::<ChunkedPolygonArray<i32, 2>>()
    }

    #[inline]
    fn as_large_polygon_opt(&self) -> Option<&ChunkedPolygonArray<i64, 2>> {
        self.as_any().downcast_ref::<ChunkedPolygonArray<i64, 2>>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&ChunkedMultiPointArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPointArray<i32, 2>>()
    }

    #[inline]
    fn as_large_multi_point_opt(&self) -> Option<&ChunkedMultiPointArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPointArray<i64, 2>>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&ChunkedMultiLineStringArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiLineStringArray<i32, 2>>()
    }

    #[inline]
    fn as_large_multi_line_string_opt(&self) -> Option<&ChunkedMultiLineStringArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiLineStringArray<i64, 2>>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&ChunkedMultiPolygonArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPolygonArray<i32, 2>>()
    }

    #[inline]
    fn as_large_multi_polygon_opt(&self) -> Option<&ChunkedMultiPolygonArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPolygonArray<i64, 2>>()
    }

    #[inline]
    fn as_mixed_opt(&self) -> Option<&ChunkedMixedGeometryArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMixedGeometryArray<i32, 2>>()
    }

    #[inline]
    fn as_large_mixed_opt(&self) -> Option<&ChunkedMixedGeometryArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedMixedGeometryArray<i64, 2>>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&ChunkedGeometryCollectionArray<i32, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedGeometryCollectionArray<i32, 2>>()
    }

    #[inline]
    fn as_large_geometry_collection_opt(&self) -> Option<&ChunkedGeometryCollectionArray<i64, 2>> {
        self.as_any()
            .downcast_ref::<ChunkedGeometryCollectionArray<i64, 2>>()
    }

    #[inline]
    fn as_wkb_opt(&self) -> Option<&ChunkedWKBArray<i32>> {
        self.as_any().downcast_ref::<ChunkedWKBArray<i32>>()
    }

    #[inline]
    fn as_large_wkb_opt(&self) -> Option<&ChunkedWKBArray<i64>> {
        self.as_any().downcast_ref::<ChunkedWKBArray<i64>>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&ChunkedRectArray> {
        self.as_any().downcast_ref::<ChunkedRectArray>()
    }
}
