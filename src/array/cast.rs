use crate::array::*;
use crate::chunked_array::*;

/// Helpers for downcasting a [`NativeArray`] to a concrete implementation.
pub trait AsNativeArray {
    /// Downcast this to a [`PointArray`] returning `None` if not possible
    fn as_point_opt<const D: usize>(&self) -> Option<&PointArray<D>>;

    /// Downcast this to a [`PointArray`] panicking if not possible
    #[inline]
    fn as_point<const D: usize>(&self) -> &PointArray<D> {
        self.as_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt<const D: usize>(&self) -> Option<&LineStringArray<D>>;

    /// Downcast this to a [`LineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string<const D: usize>(&self) -> &LineStringArray<D> {
        self.as_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt<const D: usize>(&self) -> Option<&PolygonArray<D>>;

    /// Downcast this to a [`PolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon<const D: usize>(&self) -> &PolygonArray<D> {
        self.as_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&MultiPointArray<D>>;

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point<const D: usize>(&self) -> &MultiPointArray<D> {
        self.as_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt<const D: usize>(&self) -> Option<&MultiLineStringArray<D>>;

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string<const D: usize>(&self) -> &MultiLineStringArray<D> {
        self.as_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&MultiPolygonArray<D>>;

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon<const D: usize>(&self) -> &MultiPolygonArray<D> {
        self.as_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MixedGeometryArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_mixed_opt<const D: usize>(&self) -> Option<&MixedGeometryArray<D>>;

    /// Downcast this to a [`MixedGeometryArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_mixed<const D: usize>(&self) -> &MixedGeometryArray<D> {
        self.as_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt<const D: usize>(&self) -> Option<&GeometryCollectionArray<D>>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection<const D: usize>(&self) -> &GeometryCollectionArray<D> {
        self.as_geometry_collection_opt::<D>().unwrap()
    }

    /// Downcast this to a [`RectArray`] returning `None` if not possible
    fn as_rect_opt<const D: usize>(&self) -> Option<&RectArray<D>>;

    /// Downcast this to a [`RectArray`] panicking if not possible
    #[inline]
    fn as_rect<const D: usize>(&self) -> &RectArray<D> {
        self.as_rect_opt::<D>().unwrap()
    }
}

impl AsNativeArray for &dyn NativeArray {
    #[inline]
    fn as_point_opt<const D: usize>(&self) -> Option<&PointArray<D>> {
        self.as_any().downcast_ref::<PointArray<D>>()
    }

    #[inline]
    fn as_line_string_opt<const D: usize>(&self) -> Option<&LineStringArray<D>> {
        self.as_any().downcast_ref::<LineStringArray<D>>()
    }

    #[inline]
    fn as_polygon_opt<const D: usize>(&self) -> Option<&PolygonArray<D>> {
        self.as_any().downcast_ref::<PolygonArray<D>>()
    }

    #[inline]
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&MultiPointArray<D>> {
        self.as_any().downcast_ref::<MultiPointArray<D>>()
    }

    #[inline]
    fn as_multi_line_string_opt<const D: usize>(&self) -> Option<&MultiLineStringArray<D>> {
        self.as_any().downcast_ref::<MultiLineStringArray<D>>()
    }

    #[inline]
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&MultiPolygonArray<D>> {
        self.as_any().downcast_ref::<MultiPolygonArray<D>>()
    }

    #[inline]
    fn as_mixed_opt<const D: usize>(&self) -> Option<&MixedGeometryArray<D>> {
        self.as_any().downcast_ref::<MixedGeometryArray<D>>()
    }

    #[inline]
    fn as_geometry_collection_opt<const D: usize>(&self) -> Option<&GeometryCollectionArray<D>> {
        self.as_any().downcast_ref::<GeometryCollectionArray<D>>()
    }

    #[inline]
    fn as_rect_opt<const D: usize>(&self) -> Option<&RectArray<D>> {
        self.as_any().downcast_ref::<RectArray<D>>()
    }
}

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
    fn as_point_opt<const D: usize>(&self) -> Option<&ChunkedPointArray<D>>;

    /// Downcast this to a [`ChunkedPointArray`] panicking if not possible
    #[inline]
    fn as_point<const D: usize>(&self) -> &ChunkedPointArray<D> {
        self.as_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt<const D: usize>(&self) -> Option<&ChunkedLineStringArray<D>>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string<const D: usize>(&self) -> &ChunkedLineStringArray<D> {
        self.as_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt<const D: usize>(&self) -> Option<&ChunkedPolygonArray<D>>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon<const D: usize>(&self) -> &ChunkedPolygonArray<D> {
        self.as_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&ChunkedMultiPointArray<D>>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point<const D: usize>(&self) -> &ChunkedMultiPointArray<D> {
        self.as_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt<const D: usize>(&self) -> Option<&ChunkedMultiLineStringArray<D>>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string<const D: usize>(&self) -> &ChunkedMultiLineStringArray<D> {
        self.as_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&ChunkedMultiPolygonArray<D>>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon<const D: usize>(&self) -> &ChunkedMultiPolygonArray<D> {
        self.as_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_mixed_opt<const D: usize>(&self) -> Option<&ChunkedMixedGeometryArray<D>>;

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_mixed<const D: usize>(&self) -> &ChunkedMixedGeometryArray<D> {
        self.as_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt<const D: usize>(&self) -> Option<&ChunkedGeometryCollectionArray<D>>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection<const D: usize>(&self) -> &ChunkedGeometryCollectionArray<D> {
        self.as_geometry_collection_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedRectArray`] returning `None` if not possible
    fn as_rect_opt<const D: usize>(&self) -> Option<&ChunkedRectArray<D>>;

    /// Downcast this to a [`ChunkedRectArray`] panicking if not possible
    #[inline]
    fn as_rect<const D: usize>(&self) -> &ChunkedRectArray<D> {
        self.as_rect_opt::<D>().unwrap()
    }
}

impl AsChunkedNativeArray for &dyn ChunkedNativeArray {
    #[inline]
    fn as_point_opt<const D: usize>(&self) -> Option<&ChunkedPointArray<D>> {
        self.as_any().downcast_ref::<ChunkedPointArray<D>>()
    }

    #[inline]
    fn as_line_string_opt<const D: usize>(&self) -> Option<&ChunkedLineStringArray<D>> {
        self.as_any().downcast_ref::<ChunkedLineStringArray<D>>()
    }

    #[inline]
    fn as_polygon_opt<const D: usize>(&self) -> Option<&ChunkedPolygonArray<D>> {
        self.as_any().downcast_ref::<ChunkedPolygonArray<D>>()
    }

    #[inline]
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&ChunkedMultiPointArray<D>> {
        self.as_any().downcast_ref::<ChunkedMultiPointArray<D>>()
    }

    #[inline]
    fn as_multi_line_string_opt<const D: usize>(&self) -> Option<&ChunkedMultiLineStringArray<D>> {
        self.as_any().downcast_ref::<ChunkedMultiLineStringArray<D>>()
    }

    #[inline]
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&ChunkedMultiPolygonArray<D>> {
        self.as_any().downcast_ref::<ChunkedMultiPolygonArray<D>>()
    }

    #[inline]
    fn as_mixed_opt<const D: usize>(&self) -> Option<&ChunkedMixedGeometryArray<D>> {
        self.as_any().downcast_ref::<ChunkedMixedGeometryArray<D>>()
    }

    #[inline]
    fn as_geometry_collection_opt<const D: usize>(&self) -> Option<&ChunkedGeometryCollectionArray<D>> {
        self.as_any().downcast_ref::<ChunkedGeometryCollectionArray<D>>()
    }

    #[inline]
    fn as_rect_opt<const D: usize>(&self) -> Option<&ChunkedRectArray<D>> {
        self.as_any().downcast_ref::<ChunkedRectArray<D>>()
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
