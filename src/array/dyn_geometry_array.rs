use crate::array::zip_validity::ZipValidity;
use crate::array::{
    GeometryCollectionArray, LineStringArray, MixedGeometryArray, MultiLineStringArray,
    MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray, WKBArray,
};
use crate::datatypes::GeoDataType;
use crate::error::GeoArrowError;
use crate::scalar::Geometry;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::bit_iterator::BitIterator;
use arrow_schema::Field;

impl dyn GeometryArrayTrait {
    pub fn value<O: OffsetSizeTrait>(&self, i: usize) -> Geometry<O> {
        match self.data_type() {
            GeoDataType::Point(_) => Geometry::Point(as_point_array(self).value(i)),
            GeoDataType::LineString(_) | GeoDataType::LargeLineString(_) => {
                Geometry::LineString(as_line_string_array::<O>(self).value(i))
            }
            GeoDataType::Polygon(_) | GeoDataType::LargePolygon(_) => {
                Geometry::Polygon(as_polygon_array::<O>(self).value(i))
            }
            GeoDataType::MultiPoint(_) | GeoDataType::LargeMultiPoint(_) => {
                Geometry::MultiPoint(as_multi_point_array::<O>(self).value(i))
            }
            GeoDataType::MultiLineString(_) | GeoDataType::LargeMultiLineString(_) => {
                Geometry::MultiLineString(as_multi_line_string_array::<O>(self).value(i))
            }
            GeoDataType::MultiPolygon(_) | GeoDataType::LargeMultiPolygon(_) => {
                Geometry::MultiPolygon(as_multi_polygon_array::<O>(self).value(i))
            }
            GeoDataType::Mixed(_) | GeoDataType::LargeMixed(_) => {
                as_mixed_array::<O>(self).value(i)
            }
            // GeoDataType::GeometryCollection(_) | GeoDataType::LargeGeometryCollection(_) =>
            //     as_geometry_collection_array::<O>(self).value(i),
            // GeoDataType::WKB | GeoDataType::LargeWKB => as_wkb_array::<O>(self).value(i),
            GeoDataType::Rect => Geometry::Rect(as_rect_array(self).value(i)),
            _ => unimplemented!(),
        }
    }
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

impl TryFrom<(&Field, &dyn Array, bool)> for Box<dyn GeometryArrayTrait> {
    type Error = GeoArrowError;

    fn try_from((field, array, is_large): (&Field, &dyn Array, bool)) -> Result<Self, Self::Error> {
        if is_large {
            if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
                let geom_arr: Result<Box<dyn GeometryArrayTrait>, GeoArrowError> =
                    match extension_name.as_str() {
                        "geoarrow.point" => Ok(Box::new(PointArray::try_from(array)?)),
                        "geoarrow.linestring" => {
                            Ok(Box::new(LineStringArray::<i32>::try_from(array)?))
                        }
                        "geoarrow.polygon" => Ok(Box::new(PolygonArray::<i32>::try_from(array)?)),
                        "geoarrow.multipoint" => {
                            Ok(Box::new(MultiPointArray::<i32>::try_from(array)?))
                        }
                        "geoarrow.multilinestring" => {
                            Ok(Box::new(MultiLineStringArray::<i32>::try_from(array)?))
                        }
                        "geoarrow.multipolygon" => {
                            Ok(Box::new(MultiPolygonArray::<i32>::try_from(array)?))
                        }
                        // TODO: create a top-level API that parses any named geoarrow array?
                        // "geoarrow.wkb" => Ok(GeometryArray::WKB(array.try_into()?)),
                        _ => Err(GeoArrowError::General(format!(
                            "Unknown geoarrow type {}",
                            extension_name
                        ))),
                    };
                geom_arr
            } else {
                // TODO: better error here, and document that arrays without geoarrow extension
                // metadata should use TryFrom for a specific geometry type directly, instead of using
                // GeometryArray
                Err(GeoArrowError::General(
                    "Can only construct an array with an extension type name.".to_string(),
                ))
            }
        } else {
            if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
                let geom_arr: Result<Box<dyn GeometryArrayTrait>, GeoArrowError> =
                    match extension_name.as_str() {
                        "geoarrow.point" => Ok(Box::new(PointArray::try_from(array)?)),
                        "geoarrow.linestring" => {
                            Ok(Box::new(LineStringArray::<i64>::try_from(array)?))
                        }
                        "geoarrow.polygon" => Ok(Box::new(PolygonArray::<i64>::try_from(array)?)),
                        "geoarrow.multipoint" => {
                            Ok(Box::new(MultiPointArray::<i64>::try_from(array)?))
                        }
                        "geoarrow.multilinestring" => {
                            Ok(Box::new(MultiLineStringArray::<i64>::try_from(array)?))
                        }
                        "geoarrow.multipolygon" => {
                            Ok(Box::new(MultiPolygonArray::<i64>::try_from(array)?))
                        }
                        // TODO: create a top-level API that parses any named geoarrow array?
                        // "geoarrow.wkb" => Ok(GeometryArray::WKB(array.try_into()?)),
                        _ => Err(GeoArrowError::General(format!(
                            "Unknown geoarrow type {}",
                            extension_name
                        ))),
                    };
                geom_arr
            } else {
                // TODO: better error here, and document that arrays without geoarrow extension
                // metadata should use TryFrom for a specific geometry type directly, instead of using
                // GeometryArray
                Err(GeoArrowError::General(
                    "Can only construct an array with an extension type name.".to_string(),
                ))
            }
        }
    }
}
