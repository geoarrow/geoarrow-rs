use crate::array::zip_validity::ZipValidity;
use crate::array::{
    GeometryCollectionArray, LineStringArray, MixedGeometryArray, MultiLineStringArray,
    MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray, WKBArray,
};
use crate::datatypes::GeoDataType;
use crate::trait_::GeoArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::bit_iterator::BitIterator;

impl dyn GeometryArrayTrait {
    pub fn value_as_geo(&self, i: usize) -> geo::Geometry {
        match self.data_type() {
            GeoDataType::Point(_) => geo::Geometry::Point(as_point_array(self).value_as_geo(i)),
            GeoDataType::LineString(_) => {
                geo::Geometry::LineString(as_line_string_array::<i32>(self).value_as_geo(i))
            }
            GeoDataType::LargeLineString(_) => {
                geo::Geometry::LineString(as_line_string_array::<i64>(self).value_as_geo(i))
            }
            GeoDataType::Polygon(_) => {
                geo::Geometry::Polygon(as_polygon_array::<i32>(self).value_as_geo(i))
            }
            GeoDataType::LargePolygon(_) => {
                geo::Geometry::Polygon(as_polygon_array::<i64>(self).value_as_geo(i))
            }
            GeoDataType::MultiPoint(_) => {
                geo::Geometry::MultiPoint(as_multi_point_array::<i32>(self).value_as_geo(i))
            }
            GeoDataType::LargeMultiPoint(_) => {
                geo::Geometry::MultiPoint(as_multi_point_array::<i64>(self).value_as_geo(i))
            }
            GeoDataType::MultiLineString(_) => geo::Geometry::MultiLineString(
                as_multi_line_string_array::<i32>(self).value_as_geo(i),
            ),
            GeoDataType::LargeMultiLineString(_) => geo::Geometry::MultiLineString(
                as_multi_line_string_array::<i64>(self).value_as_geo(i),
            ),
            GeoDataType::MultiPolygon(_) => {
                geo::Geometry::MultiPolygon(as_multi_polygon_array::<i32>(self).value_as_geo(i))
            }
            GeoDataType::LargeMultiPolygon(_) => {
                geo::Geometry::MultiPolygon(as_multi_polygon_array::<i64>(self).value_as_geo(i))
            }
            GeoDataType::Mixed(_) => as_mixed_array::<i32>(self).value_as_geo(i),
            GeoDataType::LargeMixed(_) => as_mixed_array::<i64>(self).value_as_geo(i),
            GeoDataType::GeometryCollection(_) => geo::Geometry::GeometryCollection(
                as_geometry_collection_array::<i32>(self).value_as_geo(i),
            ),
            GeoDataType::LargeGeometryCollection(_) => geo::Geometry::GeometryCollection(
                as_geometry_collection_array::<i64>(self).value_as_geo(i),
            ),
            GeoDataType::WKB => as_wkb_array::<i32>(self).value_as_geo(i),
            GeoDataType::LargeWKB => as_wkb_array::<i64>(self).value_as_geo(i),
            GeoDataType::Rect => geo::Geometry::Rect(as_rect_array(self).value_as_geo(i)),
        }
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Geometry, impl Iterator<Item = geo::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        match self.data_type() {
            GeoDataType::Point(_) => as_point_array(self).value_as_geos(i),
            GeoDataType::LineString(_) => as_line_string_array::<i32>(self).value_as_geos(i),
            GeoDataType::LargeLineString(_) => as_line_string_array::<i64>(self).value_as_geos(i),
            GeoDataType::Polygon(_) => as_polygon_array::<i32>(self).value_as_geos(i),
            GeoDataType::LargePolygon(_) => as_polygon_array::<i64>(self).value_as_geos(i),
            GeoDataType::MultiPoint(_) => as_multi_point_array::<i32>(self).value_as_geos(i),
            GeoDataType::LargeMultiPoint(_) => as_multi_point_array::<i64>(self).value_as_geos(i),
            GeoDataType::MultiLineString(_) => {
                as_multi_line_string_array::<i32>(self).value_as_geos(i)
            }
            GeoDataType::LargeMultiLineString(_) => {
                as_multi_line_string_array::<i64>(self).value_as_geos(i)
            }
            GeoDataType::MultiPolygon(_) => as_multi_polygon_array::<i32>(self).value_as_geos(i),
            GeoDataType::LargeMultiPolygon(_) => {
                as_multi_polygon_array::<i64>(self).value_as_geos(i)
            }
            GeoDataType::Mixed(_) => as_mixed_array::<i32>(self).value_as_geos(i),
            GeoDataType::LargeMixed(_) => as_mixed_array::<i64>(self).value_as_geos(i),
            GeoDataType::GeometryCollection(_) => {
                as_geometry_collection_array::<i32>(self).value_as_geos(i)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                as_geometry_collection_array::<i64>(self).value_as_geos(i)
            }
            GeoDataType::WKB => as_wkb_array::<i32>(self).value_as_geos(i),
            GeoDataType::LargeWKB => as_wkb_array::<i64>(self).value_as_geos(i),
            GeoDataType::Rect => todo!(),
        }
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(
        &self,
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitIterator> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.nulls())
    }
}

#[inline]
pub fn as_point_array(arr: &dyn GeometryArrayTrait) -> &PointArray {
    arr.as_any()
        .downcast_ref::<PointArray>()
        .expect("Unable to downcast to point array")
}

#[inline]
pub fn as_line_string_array<O: OffsetSizeTrait>(
    arr: &dyn GeometryArrayTrait,
) -> &LineStringArray<O> {
    arr.as_any()
        .downcast_ref::<LineStringArray<O>>()
        .expect("Unable to downcast to line string array")
}

#[inline]
pub fn as_polygon_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &PolygonArray<O> {
    arr.as_any()
        .downcast_ref::<PolygonArray<O>>()
        .expect("Unable to downcast to polygon array")
}

#[inline]
pub fn as_multi_point_array<O: OffsetSizeTrait>(
    arr: &dyn GeometryArrayTrait,
) -> &MultiPointArray<O> {
    arr.as_any()
        .downcast_ref::<MultiPointArray<O>>()
        .expect("Unable to downcast to multi point array")
}

#[inline]
pub fn as_multi_line_string_array<O: OffsetSizeTrait>(
    arr: &dyn GeometryArrayTrait,
) -> &MultiLineStringArray<O> {
    arr.as_any()
        .downcast_ref::<MultiLineStringArray<O>>()
        .expect("Unable to downcast to multi line string array")
}

#[inline]
pub fn as_multi_polygon_array<O: OffsetSizeTrait>(
    arr: &dyn GeometryArrayTrait,
) -> &MultiPolygonArray<O> {
    arr.as_any()
        .downcast_ref::<MultiPolygonArray<O>>()
        .expect("Unable to downcast to multi polygon array")
}

#[inline]
pub fn as_rect_array(arr: &dyn GeometryArrayTrait) -> &RectArray {
    arr.as_any()
        .downcast_ref::<RectArray>()
        .expect("Unable to downcast to rect array")
}

#[inline]
pub fn as_wkb_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &WKBArray<O> {
    arr.as_any()
        .downcast_ref::<WKBArray<O>>()
        .expect("Unable to downcast to wkb array")
}

#[inline]
pub fn as_mixed_array<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> &MixedGeometryArray<O> {
    arr.as_any()
        .downcast_ref::<MixedGeometryArray<O>>()
        .expect("Unable to downcast to mixed array")
}

#[inline]
pub fn as_geometry_collection_array<O: OffsetSizeTrait>(
    arr: &dyn GeometryArrayTrait,
) -> &GeometryCollectionArray<O> {
    arr.as_any()
        .downcast_ref::<GeometryCollectionArray<O>>()
        .expect("Unable to downcast to geometry collection array")
}
