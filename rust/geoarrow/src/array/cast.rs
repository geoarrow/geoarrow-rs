use crate::array::*;
use crate::chunked_array::*;

/// Helpers for downcasting a [`NativeArray`] to a concrete implementation.
pub trait AsNativeArray {
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
}

impl<T: NativeArray> AsNativeArray for T {
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
}

// #[deprecated]
impl AsNativeArray for &dyn NativeArray {
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
}

/// Trait to downcast an Arrow array to a serialized array
pub trait AsSerializedArray {
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

impl AsSerializedArray for &dyn SerializedArray {
    #[inline]
    fn as_wkb_opt(&self) -> Option<&WKBArray<i32>> {
        self.as_any().downcast_ref::<WKBArray<i32>>()
    }

    #[inline]
    fn as_large_wkb_opt(&self) -> Option<&WKBArray<i64>> {
        self.as_any().downcast_ref::<WKBArray<i64>>()
    }
}

/// Helpers for downcasting a [`ChunkedNativeArray`] to a concrete implementation.
pub trait AsChunkedNativeArray {
    /// Downcast this to a [`ChunkedPointArray`] returning `None` if not possible
    fn as_point_opt(&self) -> Option<&ChunkedPointArray>;

    /// Downcast this to a [`ChunkedPointArray`] panicking if not possible
    #[inline]
    fn as_point(&self) -> &ChunkedPointArray {
        self.as_point_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt(&self) -> Option<&ChunkedLineStringArray>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string(&self) -> &ChunkedLineStringArray {
        self.as_line_string_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt(&self) -> Option<&ChunkedPolygonArray>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon(&self) -> &ChunkedPolygonArray {
        self.as_polygon_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt(&self) -> Option<&ChunkedMultiPointArray>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point(&self) -> &ChunkedMultiPointArray {
        self.as_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt(&self) -> Option<&ChunkedMultiLineStringArray>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string(&self) -> &ChunkedMultiLineStringArray {
        self.as_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt(&self) -> Option<&ChunkedMultiPolygonArray>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon(&self) -> &ChunkedMultiPolygonArray {
        self.as_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt(&self) -> Option<&ChunkedGeometryCollectionArray>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection(&self) -> &ChunkedGeometryCollectionArray {
        self.as_geometry_collection_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedRectArray`] returning `None` if not possible
    fn as_rect_opt(&self) -> Option<&ChunkedRectArray>;

    /// Downcast this to a [`ChunkedRectArray`] panicking if not possible
    #[inline]
    fn as_rect(&self) -> &ChunkedRectArray {
        self.as_rect_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedUnknownGeometryArray`] returning `None` if not possible
    fn as_geometry_opt(&self) -> Option<&ChunkedUnknownGeometryArray>;

    /// Downcast this to a [`ChunkedUnknownGeometryArray`] panicking if not possible
    #[inline]
    fn as_geometry(&self) -> &ChunkedUnknownGeometryArray {
        self.as_geometry_opt().unwrap()
    }
}

impl AsChunkedNativeArray for &dyn ChunkedNativeArray {
    #[inline]
    fn as_point_opt(&self) -> Option<&ChunkedPointArray> {
        self.as_any().downcast_ref::<ChunkedPointArray>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&ChunkedLineStringArray> {
        self.as_any().downcast_ref::<ChunkedLineStringArray>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&ChunkedPolygonArray> {
        self.as_any().downcast_ref::<ChunkedPolygonArray>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&ChunkedMultiPointArray> {
        self.as_any().downcast_ref::<ChunkedMultiPointArray>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&ChunkedMultiLineStringArray> {
        self.as_any().downcast_ref::<ChunkedMultiLineStringArray>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&ChunkedMultiPolygonArray> {
        self.as_any().downcast_ref::<ChunkedMultiPolygonArray>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&ChunkedGeometryCollectionArray> {
        self.as_any()
            .downcast_ref::<ChunkedGeometryCollectionArray>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&ChunkedRectArray> {
        self.as_any().downcast_ref::<ChunkedRectArray>()
    }

    #[inline]
    fn as_geometry_opt(&self) -> Option<&ChunkedUnknownGeometryArray> {
        self.as_any().downcast_ref::<ChunkedUnknownGeometryArray>()
    }
}

#[allow(dead_code)]
pub trait AsChunkedSerializedArray {
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
}

// impl AsChunkedSerializedArray for &dyn ChunkedNativeArray {
//     #[inline]
//     fn as_wkb_opt(&self) -> Option<&ChunkedWKBArray<i32>> {
//         self.as_any().downcast_ref::<ChunkedWKBArray<i32>>()
//     }

//     #[inline]
//     fn as_large_wkb_opt(&self) -> Option<&ChunkedWKBArray<i64>> {
//         self.as_any().downcast_ref::<ChunkedWKBArray<i64>>()
//     }
// }
