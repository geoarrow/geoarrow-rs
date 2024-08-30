use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;

/// Helpers for downcasting a [`GeometryArrayTrait`] to a concrete implementation.
pub trait AsGeometryArray {
    /// Downcast this to a [`PointArray`] returning `None` if not possible
    fn as_point_opt<const D: usize>(&self) -> Option<&PointArray<D>>;

    /// Downcast this to a [`PointArray`] panicking if not possible
    #[inline]
    fn as_point<const D: usize>(&self) -> &PointArray<D> {
        self.as_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `O` offsets returning `None` if not possible
    fn as_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&LineStringArray<O, D>>;

    /// Downcast this to a [`LineStringArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_line_string<O: OffsetSizeTrait, const D: usize>(&self) -> &LineStringArray<O, D> {
        self.as_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `O` offsets returning `None` if not possible
    fn as_polygon_opt<O: OffsetSizeTrait, const D: usize>(&self) -> Option<&PolygonArray<O, D>>;

    /// Downcast this to a [`PolygonArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_polygon<O: OffsetSizeTrait, const D: usize>(&self) -> &PolygonArray<O, D> {
        self.as_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `O` offsets returning `None` if not possible
    fn as_multi_point_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MultiPointArray<O, D>>;

    /// Downcast this to a [`MultiPointArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_multi_point<O: OffsetSizeTrait, const D: usize>(&self) -> &MultiPointArray<O, D> {
        self.as_multi_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MultiLineStringArray<O, D>>;

    /// Downcast this to a [`MultiLineStringArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> &MultiLineStringArray<O, D> {
        self.as_multi_line_string_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MultiPolygonArray<O, D>>;

    /// Downcast this to a [`MultiPolygonArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon<O: OffsetSizeTrait, const D: usize>(&self) -> &MultiPolygonArray<O, D> {
        self.as_multi_polygon_opt::<D>().unwrap()
    }

    /// Downcast this to a [`MixedGeometryArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_mixed_opt<O: OffsetSizeTrait, const D: usize>(&self)
        -> Option<&MixedGeometryArray<O, D>>;

    /// Downcast this to a [`MixedGeometryArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_mixed<O: OffsetSizeTrait, const D: usize>(&self) -> &MixedGeometryArray<O, D> {
        self.as_mixed_opt::<D>().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&GeometryCollectionArray<O, D>>;

    /// Downcast this to a [`GeometryCollectionArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> &GeometryCollectionArray<O, D> {
        self.as_geometry_collection_opt::<D>().unwrap()
    }

    /// Downcast this to a [`WKBArray`] with `O` offsets returning `None` if not possible
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&WKBArray<O>>;

    /// Downcast this to a [`WKBArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_wkb<O: OffsetSizeTrait>(&self) -> &WKBArray<O> {
        self.as_wkb_opt().unwrap()
    }

    /// Downcast this to a [`RectArray`] returning `None` if not possible
    fn as_rect_opt<const D: usize>(&self) -> Option<&RectArray<D>>;

    /// Downcast this to a [`RectArray`] panicking if not possible
    #[inline]
    fn as_rect<const D: usize>(&self) -> &RectArray<D> {
        self.as_rect_opt::<D>().unwrap()
    }
}

impl AsGeometryArray for &dyn GeometryArrayTrait {
    #[inline]
    fn as_point_opt<const D: usize>(&self) -> Option<&PointArray<D>> {
        self.as_any().downcast_ref::<PointArray<D>>()
    }

    #[inline]
    fn as_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&LineStringArray<O, D>> {
        self.as_any().downcast_ref::<LineStringArray<O, D>>()
    }

    #[inline]
    fn as_polygon_opt<O: OffsetSizeTrait, const D: usize>(&self) -> Option<&PolygonArray<O, D>> {
        self.as_any().downcast_ref::<PolygonArray<O, D>>()
    }

    #[inline]
    fn as_multi_point_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MultiPointArray<O, D>> {
        self.as_any().downcast_ref::<MultiPointArray<O, D>>()
    }

    #[inline]
    fn as_multi_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MultiLineStringArray<O, D>> {
        self.as_any().downcast_ref::<MultiLineStringArray<O, D>>()
    }

    #[inline]
    fn as_multi_polygon_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MultiPolygonArray<O, D>> {
        self.as_any().downcast_ref::<MultiPolygonArray<O, D>>()
    }

    #[inline]
    fn as_mixed_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&MixedGeometryArray<O, D>> {
        self.as_any().downcast_ref::<MixedGeometryArray<O, D>>()
    }

    #[inline]
    fn as_geometry_collection_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&GeometryCollectionArray<O, D>> {
        self.as_any()
            .downcast_ref::<GeometryCollectionArray<O, D>>()
    }

    #[inline]
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&WKBArray<O>> {
        self.as_any().downcast_ref::<WKBArray<O>>()
    }

    #[inline]
    fn as_rect_opt<const D: usize>(&self) -> Option<&RectArray<D>> {
        self.as_any().downcast_ref::<RectArray<D>>()
    }
}

/// Helpers for downcasting a [`ChunkedGeometryArrayTrait`] to a concrete implementation.
pub trait AsChunkedGeometryArray {
    /// Downcast this to a [`ChunkedPointArray`] returning `None` if not possible
    fn as_point_opt<const D: usize>(&self) -> Option<&ChunkedPointArray<D>>;

    /// Downcast this to a [`ChunkedPointArray`] panicking if not possible
    #[inline]
    fn as_point<const D: usize>(&self) -> &ChunkedPointArray<D> {
        self.as_point_opt::<D>().unwrap()
    }

    /// Downcast this to a [`ChunkedLineStringArray`] with `O` offsets returning `None` if not possible
    fn as_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedLineStringArray<O, D>>;

    /// Downcast this to a [`ChunkedLineStringArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_line_string<O: OffsetSizeTrait, const D: usize>(&self) -> &ChunkedLineStringArray<O, D> {
        self.as_line_string_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedPolygonArray`] with `O` offsets returning `None` if not possible
    fn as_polygon_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedPolygonArray<O, D>>;

    /// Downcast this to a [`ChunkedPolygonArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_polygon<O: OffsetSizeTrait, const D: usize>(&self) -> &ChunkedPolygonArray<O, D> {
        self.as_polygon_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPointArray`] with `O` offsets returning `None` if not possible
    fn as_multi_point_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiPointArray<O, D>>;

    /// Downcast this to a [`ChunkedMultiPointArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_multi_point<O: OffsetSizeTrait, const D: usize>(&self) -> &ChunkedMultiPointArray<O, D> {
        self.as_multi_point_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiLineStringArray<O, D>>;

    /// Downcast this to a [`ChunkedMultiLineStringArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> &ChunkedMultiLineStringArray<O, D> {
        self.as_multi_line_string_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiPolygonArray<O, D>>;

    /// Downcast this to a [`ChunkedMultiPolygonArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> &ChunkedMultiPolygonArray<O, D> {
        self.as_multi_polygon_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_mixed_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMixedGeometryArray<O, D>>;

    /// Downcast this to a [`ChunkedMixedGeometryArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_mixed<O: OffsetSizeTrait, const D: usize>(&self) -> &ChunkedMixedGeometryArray<O, D> {
        self.as_mixed_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `O` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedGeometryCollectionArray<O, D>>;

    /// Downcast this to a [`ChunkedGeometryCollectionArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> &ChunkedGeometryCollectionArray<O, D> {
        self.as_geometry_collection_opt::<O, D>().unwrap()
    }

    /// Downcast this to a [`ChunkedWKBArray`] with `O` offsets returning `None` if not possible
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&ChunkedWKBArray<O>>;

    /// Downcast this to a [`ChunkedWKBArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_wkb<O: OffsetSizeTrait>(&self) -> &ChunkedWKBArray<O> {
        self.as_wkb_opt().unwrap()
    }

    /// Downcast this to a [`ChunkedRectArray`] returning `None` if not possible
    fn as_rect_opt<const D: usize>(&self) -> Option<&ChunkedRectArray<D>>;

    /// Downcast this to a [`ChunkedRectArray`] panicking if not possible
    #[inline]
    fn as_rect<const D: usize>(&self) -> &ChunkedRectArray<D> {
        self.as_rect_opt::<D>().unwrap()
    }
}

impl AsChunkedGeometryArray for &dyn ChunkedGeometryArrayTrait {
    #[inline]
    fn as_point_opt<const D: usize>(&self) -> Option<&ChunkedPointArray<D>> {
        self.as_any().downcast_ref::<ChunkedPointArray<D>>()
    }

    #[inline]
    fn as_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedLineStringArray<O, D>> {
        self.as_any().downcast_ref::<ChunkedLineStringArray<O, D>>()
    }

    #[inline]
    fn as_polygon_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedPolygonArray<O, D>> {
        self.as_any().downcast_ref::<ChunkedPolygonArray<O, D>>()
    }

    #[inline]
    fn as_multi_point_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiPointArray<O, D>> {
        self.as_any().downcast_ref::<ChunkedMultiPointArray<O, D>>()
    }

    #[inline]
    fn as_multi_line_string_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiLineStringArray<O, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiLineStringArray<O, D>>()
    }

    #[inline]
    fn as_multi_polygon_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMultiPolygonArray<O, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMultiPolygonArray<O, D>>()
    }

    #[inline]
    fn as_mixed_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedMixedGeometryArray<O, D>> {
        self.as_any()
            .downcast_ref::<ChunkedMixedGeometryArray<O, D>>()
    }

    #[inline]
    fn as_geometry_collection_opt<O: OffsetSizeTrait, const D: usize>(
        &self,
    ) -> Option<&ChunkedGeometryCollectionArray<O, D>> {
        self.as_any()
            .downcast_ref::<ChunkedGeometryCollectionArray<O, D>>()
    }

    #[inline]
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&ChunkedWKBArray<O>> {
        self.as_any().downcast_ref::<ChunkedWKBArray<O>>()
    }

    #[inline]
    fn as_rect_opt<const D: usize>(&self) -> Option<&ChunkedRectArray<D>> {
        self.as_any().downcast_ref::<ChunkedRectArray<D>>()
    }
}
