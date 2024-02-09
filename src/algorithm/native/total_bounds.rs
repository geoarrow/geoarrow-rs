use arrow_array::OffsetSizeTrait;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;

/// Computes the total bounds (extent) of the input.
pub trait TotalBounds {
    fn total_bounds(&self) -> BoundingRect;
}

impl TotalBounds for PointArray {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_point(&geom);
        }
        bounds
    }
}

impl TotalBounds for RectArray {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_rect(&geom);
        }
        bounds
    }
}

macro_rules! impl_array {
    ($type:ty, $func:ident) => {
        impl<O: OffsetSizeTrait> TotalBounds for $type {
            fn total_bounds(&self) -> BoundingRect {
                let mut bounds = BoundingRect::new();
                for geom in self.iter().flatten() {
                    bounds.$func(&geom);
                }
                bounds
            }
        }
    };
}

impl_array!(LineStringArray<O>, add_line_string);
impl_array!(PolygonArray<O>, add_polygon);
impl_array!(MultiPointArray<O>, add_multi_point);
impl_array!(MultiLineStringArray<O>, add_multi_line_string);
impl_array!(MultiPolygonArray<O>, add_multi_polygon);
impl_array!(MixedGeometryArray<O>, add_geometry);
impl_array!(GeometryCollectionArray<O>, add_geometry_collection);

impl<O: OffsetSizeTrait> TotalBounds for WKBArray<O> {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_geometry(&geom.to_wkb_object());
        }
        bounds
    }
}

impl TotalBounds for &dyn GeometryArrayTrait {
    fn total_bounds(&self) -> BoundingRect {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().total_bounds(),
            GeoDataType::LineString(_) => self.as_line_string().total_bounds(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().total_bounds(),
            GeoDataType::Polygon(_) => self.as_polygon().total_bounds(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().total_bounds(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().total_bounds(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().total_bounds(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().total_bounds(),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().total_bounds()
            }
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().total_bounds(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().total_bounds(),
            GeoDataType::Mixed(_) => self.as_mixed().total_bounds(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().total_bounds(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().total_bounds(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().total_bounds()
            }
            GeoDataType::Rect => self.as_rect().total_bounds(),
            GeoDataType::WKB => self.as_wkb().total_bounds(),
            GeoDataType::LargeWKB => self.as_large_wkb().total_bounds(),
        }
    }
}

impl<G: GeometryArrayTrait> TotalBounds for ChunkedGeometryArray<G> {
    fn total_bounds(&self) -> BoundingRect {
        let bounding_rects = self.map(|chunk| chunk.as_ref().total_bounds());
        bounding_rects
            .into_iter()
            .fold(BoundingRect::default(), |acc, x| acc + x)
    }
}

impl TotalBounds for &dyn ChunkedGeometryArrayTrait {
    fn total_bounds(&self) -> BoundingRect {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().total_bounds(),
            GeoDataType::LineString(_) => self.as_line_string().total_bounds(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().total_bounds(),
            GeoDataType::Polygon(_) => self.as_polygon().total_bounds(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().total_bounds(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().total_bounds(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().total_bounds(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().total_bounds(),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().total_bounds()
            }
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().total_bounds(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().total_bounds(),
            GeoDataType::Mixed(_) => self.as_mixed().total_bounds(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().total_bounds(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().total_bounds(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().total_bounds()
            }
            GeoDataType::Rect => self.as_rect().total_bounds(),
            GeoDataType::WKB => self.as_wkb().total_bounds(),
            GeoDataType::LargeWKB => self.as_large_wkb().total_bounds(),
        }
    }
}
