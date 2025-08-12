use arrow_array::Array;
use arrow_json::writer::NullableEncoder;
use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
use arrow_schema::{ArrowError, FieldRef};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_schema::GeoArrowType;

use crate::encoder::geometry::GeometryEncoder;
use crate::encoder::geometrycollection::GeometryCollectionEncoder;
use crate::encoder::linestring::LineStringEncoder;
use crate::encoder::multilinestring::MultiLineStringEncoder;
use crate::encoder::multipoint::MultiPointEncoder;
use crate::encoder::multipolygon::MultiPolygonEncoder;
use crate::encoder::point::PointEncoder;
use crate::encoder::polygon::PolygonEncoder;
use crate::encoder::rect::RectEncoder;
use crate::encoder::wkb::{GenericWkbEncoder, WkbViewEncoder};
use crate::encoder::wkt::{GenericWktEncoder, WktViewEncoder};

/// A [EncoderFactory] for writing GeoArrow arrays.
#[derive(Debug)]
pub struct GeoArrowEncoderFactory;

impl EncoderFactory for GeoArrowEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        if let Ok(geometry_array) = from_arrow_array(array, field) {
            let nulls = array.logical_nulls();

            let encoder: Box<dyn Encoder> = match geometry_array.data_type() {
                GeoArrowType::Point(_) => {
                    Box::new(PointEncoder::new(geometry_array.as_point().clone()))
                }
                GeoArrowType::LineString(_) => Box::new(LineStringEncoder::new(
                    geometry_array.as_line_string().clone(),
                )),
                GeoArrowType::Polygon(_) => {
                    Box::new(PolygonEncoder::new(geometry_array.as_polygon().clone()))
                }
                GeoArrowType::MultiPoint(_) => Box::new(MultiPointEncoder::new(
                    geometry_array.as_multi_point().clone(),
                )),
                GeoArrowType::MultiLineString(_) => Box::new(MultiLineStringEncoder::new(
                    geometry_array.as_multi_line_string().clone(),
                )),
                GeoArrowType::MultiPolygon(_) => Box::new(MultiPolygonEncoder::new(
                    geometry_array.as_multi_polygon().clone(),
                )),
                GeoArrowType::Geometry(_) => {
                    Box::new(GeometryEncoder::new(geometry_array.as_geometry().clone()))
                }
                GeoArrowType::GeometryCollection(_) => Box::new(GeometryCollectionEncoder::new(
                    geometry_array.as_geometry_collection().clone(),
                )),
                GeoArrowType::Wkb(_) => Box::new(GenericWkbEncoder::new(
                    geometry_array.as_wkb::<i32>().clone(),
                )),
                GeoArrowType::LargeWkb(_) => Box::new(GenericWkbEncoder::new(
                    geometry_array.as_wkb::<i64>().clone(),
                )),
                GeoArrowType::WkbView(_) => {
                    Box::new(WkbViewEncoder::new(geometry_array.as_wkb_view().clone()))
                }
                GeoArrowType::Wkt(_) => Box::new(GenericWktEncoder::new(
                    geometry_array.as_wkt::<i32>().clone(),
                )),
                GeoArrowType::LargeWkt(_) => Box::new(GenericWktEncoder::new(
                    geometry_array.as_wkt::<i64>().clone(),
                )),
                GeoArrowType::WktView(_) => {
                    Box::new(WktViewEncoder::new(geometry_array.as_wkt_view().clone()))
                }
                GeoArrowType::Rect(_) => {
                    Box::new(RectEncoder::new(geometry_array.as_rect().clone()))
                }
            };
            Ok(Some(NullableEncoder::new(encoder, nulls)))
        } else {
            Ok(None)
        }
    }
}
