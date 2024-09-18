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
    fn as_line_string_opt<const D: usize>(&self) -> Option<&LineStringArray<i32, D>>;

    /// Downcast this to a [`LineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string<const D: usize>(&self) -> &LineStringArray<i32, D> {
        self.as_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `i64` offsets returning `None` if not possible
    fn as_large_line_string_opt<const D: usize>(&self) -> Option<&LineStringArray<i64, D>>;

    /// Downcast this to a [`LineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_line_string<const D: usize>(&self) -> &LineStringArray<i64, D> {
        self.as_large_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt<const D: usize>(&self) -> Option<&PolygonArray<i32, D>>;

    /// Downcast this to a [`PolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon<const D: usize>(&self) -> &PolygonArray<i32, D> {
        self.as_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i64` offsets returning `None` if not possible
    fn as_large_polygon_opt<const D: usize>(&self) -> Option<&PolygonArray<i64, D>>;

    /// Downcast this to a [`PolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_polygon<const D: usize>(&self) -> &PolygonArray<i64, D> {
        self.as_large_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&MultiPointArray<i32, D>>;

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point<const D: usize>(&self) -> &MultiPointArray<i32, D> {
        self.as_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i64` offsets returning `None` if not possible
    fn as_large_multi_point_opt<const D: usize>(&self) -> Option<&MultiPointArray<i64, D>>;

    /// Downcast this to a [`MultiPointArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_point<const D: usize>(&self) -> &MultiPointArray<i64, D> {
        self.as_large_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt<const D: usize>(&self) -> Option<&MultiLineStringArray<i32, D>>;

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string<const D: usize>(&self) -> &MultiLineStringArray<i32, D> {
        self.as_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_line_string_opt<const D: usize>(
        &self,
    ) -> Option<&MultiLineStringArray<i64, D>>;

    /// Downcast this to a [`MultiLineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_line_string<const D: usize>(&self) -> &MultiLineStringArray<i64, D> {
        self.as_large_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&MultiPolygonArray<i32, D>>;

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon<const D: usize>(&self) -> &MultiPolygonArray<i32, D> {
        self.as_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_polygon_opt<const D: usize>(&self) -> Option<&MultiPolygonArray<i64, D>>;

    /// Downcast this to a [`MultiPolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_polygon<const D: usize>(&self) -> &MultiPolygonArray<i64, D> {
        self.as_large_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MixedGeometryArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_mixed_opt<const D: usize>(&self) -> Option<&MixedGeometryArray<i32, D>>;

    /// Downcast this to a [`MixedGeometryArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_mixed<const D: usize>(&self) -> &MixedGeometryArray<i32, D> {
        self.as_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MixedGeometryArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_mixed_opt<const D: usize>(&self) -> Option<&MixedGeometryArray<i64, D>>;

    /// Downcast this to a [`MixedGeometryArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_mixed<const D: usize>(&self) -> &MixedGeometryArray<i64, D> {
        self.as_large_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&GeometryCollectionArray<i32, D>>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection<const D: usize>(&self) -> &GeometryCollectionArray<i32, D> {
        self.as_geometry_collection_opt::<D>().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&GeometryCollectionArray<i64, D>>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_geometry_collection<const D: usize>(&self) -> &GeometryCollectionArray<i64, D> {
        self.as_large_geometry_collection_opt::<D>().unwrap()
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
    fn as_line_string_opt<const D: usize>(&self) -> Option<&LineStringArray<i32, D>> {
        self.as_any().downcast_ref::<LineStringArray<i32, D>>()
    }

    #[inline]
    fn as_large_line_string_opt<const D: usize>(&self) -> Option<&LineStringArray<i64, D>> {
        self.as_any().downcast_ref::<LineStringArray<i64, D>>()
    }

    #[inline]
    fn as_polygon_opt<const D: usize>(&self) -> Option<&PolygonArray<i32, D>> {
        self.as_any().downcast_ref::<PolygonArray<i32, D>>()
    }

    #[inline]
    fn as_large_polygon_opt<const D: usize>(&self) -> Option<&PolygonArray<i64, D>> {
        self.as_any().downcast_ref::<PolygonArray<i64, D>>()
    }

    #[inline]
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&MultiPointArray<i32, D>> {
        self.as_any().downcast_ref::<MultiPointArray<i32, D>>()
    }

    #[inline]
    fn as_large_multi_point_opt<const D: usize>(&self) -> Option<&MultiPointArray<i64, D>> {
        self.as_any().downcast_ref::<MultiPointArray<i64, D>>()
    }

    #[inline]
    fn as_multi_line_string_opt<const D: usize>(&self) -> Option<&MultiLineStringArray<i32, D>> {
        self.as_any().downcast_ref::<MultiLineStringArray<i32, D>>()
    }

    #[inline]
    fn as_large_multi_line_string_opt<const D: usize>(
        &self,
    ) -> Option<&MultiLineStringArray<i64, D>> {
        self.as_any().downcast_ref::<MultiLineStringArray<i64, D>>()
    }

    #[inline]
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&MultiPolygonArray<i32, D>> {
        self.as_any().downcast_ref::<MultiPolygonArray<i32, D>>()
    }

    #[inline]
    fn as_large_multi_polygon_opt<const D: usize>(&self) -> Option<&MultiPolygonArray<i64, D>> {
        self.as_any().downcast_ref::<MultiPolygonArray<i64, D>>()
    }

    #[inline]
    fn as_mixed_opt<const D: usize>(&self) -> Option<&MixedGeometryArray<i32, D>> {
        self.as_any().downcast_ref::<MixedGeometryArray<i32, D>>()
    }

    #[inline]
    fn as_large_mixed_opt<const D: usize>(&self) -> Option<&MixedGeometryArray<i64, D>> {
        self.as_any().downcast_ref::<MixedGeometryArray<i64, D>>()
    }

    #[inline]
    fn as_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&GeometryCollectionArray<i32, D>> {
        self.as_any()
            .downcast_ref::<GeometryCollectionArray<i32, D>>()
    }

    #[inline]
    fn as_large_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&GeometryCollectionArray<i64, D>> {
        self.as_any()
            .downcast_ref::<GeometryCollectionArray<i64, D>>()
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
    fn as_rect_opt<const D: usize>(&self) -> Option<&RectArray<D>> {
        self.as_any().downcast_ref::<RectArray<D>>()
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
    fn as_line_string_opt<const D: usize>(&self) -> Option<&ChunkedLineStringArray<i32, D>>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string<const D: usize>(&self) -> &ChunkedLineStringArray<i32, D> {
        self.as_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedLineStringArray`] with `i64` offsets returning `None` if not possible
    fn as_large_line_string_opt<const D: usize>(&self) -> Option<&ChunkedLineStringArray<i64, D>>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_line_string<const D: usize>(&self) -> &ChunkedLineStringArray<i64, D> {
        self.as_large_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt<const D: usize>(&self) -> Option<&ChunkedPolygonArray<i32, D>>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon<const D: usize>(&self) -> &ChunkedPolygonArray<i32, D> {
        self.as_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `i64` offsets returning `None` if not possible
    fn as_large_polygon_opt<const D: usize>(&self) -> Option<&ChunkedPolygonArray<i64, D>>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_polygon<const D: usize>(&self) -> &ChunkedPolygonArray<i64, D> {
        self.as_large_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&ChunkedMultiPointArray<i32, D>>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point<const D: usize>(&self) -> &ChunkedMultiPointArray<i32, D> {
        self.as_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i64` offsets returning `None` if not possible
    fn as_large_multi_point_opt<const D: usize>(&self) -> Option<&ChunkedMultiPointArray<i64, D>>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_point<const D: usize>(&self) -> &ChunkedMultiPointArray<i64, D> {
        self.as_large_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiLineStringArray<i32, D>>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string<const D: usize>(&self) -> &ChunkedMultiLineStringArray<i32, D> {
        self.as_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_line_string_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiLineStringArray<i64, D>>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_line_string<const D: usize>(&self) -> &ChunkedMultiLineStringArray<i64, D> {
        self.as_large_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&ChunkedMultiPolygonArray<i32, D>>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon<const D: usize>(&self) -> &ChunkedMultiPolygonArray<i32, D> {
        self.as_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_multi_polygon_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiPolygonArray<i64, D>>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_multi_polygon<const D: usize>(&self) -> &ChunkedMultiPolygonArray<i64, D> {
        self.as_large_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_mixed_opt<const D: usize>(&self) -> Option<&ChunkedMixedGeometryArray<i32, D>>;

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_mixed<const D: usize>(&self) -> &ChunkedMixedGeometryArray<i32, D> {
        self.as_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_mixed_opt<const D: usize>(&self) -> Option<&ChunkedMixedGeometryArray<i64, D>>;

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_mixed<const D: usize>(&self) -> &ChunkedMixedGeometryArray<i64, D> {
        self.as_large_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedGeometryCollectionArray<i32, D>>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection<const D: usize>(&self) -> &ChunkedGeometryCollectionArray<i32, D> {
        self.as_geometry_collection_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i64` offsets returning `None` if not
    /// possible
    fn as_large_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedGeometryCollectionArray<i64, D>>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `i64` offsets panicking if not possible
    #[inline]
    fn as_large_geometry_collection<const D: usize>(
        &self,
    ) -> &ChunkedGeometryCollectionArray<i64, D> {
        self.as_large_geometry_collection_opt::<D>().unwrap()
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
    fn as_line_string_opt<const D: usize>(&self) -> Option<&ChunkedLineStringArray<i32, D>> {
        self.as_any()
            .downcast_ref::<ChunkedLineStringArray<i32, D>>()
    }

    #[inline]
    fn as_large_line_string_opt<const D: usize>(&self) -> Option<&ChunkedLineStringArray<i64, D>> {
        self.as_any()
            .downcast_ref::<ChunkedLineStringArray<i64, D>>()
    }

    #[inline]
    fn as_polygon_opt<const D: usize>(&self) -> Option<&ChunkedPolygonArray<i32, D>> {
        self.as_any().downcast_ref::<ChunkedPolygonArray<i32, D>>()
    }

    #[inline]
    fn as_large_polygon_opt<const D: usize>(&self) -> Option<&ChunkedPolygonArray<i64, D>> {
        self.as_any().downcast_ref::<ChunkedPolygonArray<i64, D>>()
    }

    #[inline]
    fn as_multi_point_opt<const D: usize>(&self) -> Option<&ChunkedMultiPointArray<i32, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPointArray<i32, D>>()
    }

    #[inline]
    fn as_large_multi_point_opt<const D: usize>(&self) -> Option<&ChunkedMultiPointArray<i64, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPointArray<i64, D>>()
    }

    #[inline]
    fn as_multi_line_string_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiLineStringArray<i32, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiLineStringArray<i32, D>>()
    }

    #[inline]
    fn as_large_multi_line_string_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiLineStringArray<i64, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiLineStringArray<i64, D>>()
    }

    #[inline]
    fn as_multi_polygon_opt<const D: usize>(&self) -> Option<&ChunkedMultiPolygonArray<i32, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPolygonArray<i32, D>>()
    }

    #[inline]
    fn as_large_multi_polygon_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiPolygonArray<i64, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPolygonArray<i64, D>>()
    }

    #[inline]
    fn as_mixed_opt<const D: usize>(&self) -> Option<&ChunkedMixedGeometryArray<i32, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMixedGeometryArray<i32, D>>()
    }

    #[inline]
    fn as_large_mixed_opt<const D: usize>(&self) -> Option<&ChunkedMixedGeometryArray<i64, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMixedGeometryArray<i64, D>>()
    }

    #[inline]
    fn as_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedGeometryCollectionArray<i32, D>> {
        self.as_any()
            .downcast_ref::<ChunkedGeometryCollectionArray<i32, D>>()
    }

    #[inline]
    fn as_large_geometry_collection_opt<const D: usize>(
        &self,
    ) -> Option<&ChunkedGeometryCollectionArray<i64, D>> {
        self.as_any()
            .downcast_ref::<ChunkedGeometryCollectionArray<i64, D>>()
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
    fn as_rect_opt<const D: usize>(&self) -> Option<&ChunkedRectArray<D>> {
        self.as_any().downcast_ref::<ChunkedRectArray<D>>()
    }
}
