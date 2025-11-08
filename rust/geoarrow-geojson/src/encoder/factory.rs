use arrow_array::Array;
use arrow_json::writer::NullableEncoder;
use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
use arrow_schema::{ArrowError, FieldRef};
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
        if let Some(geoarrow_type) = GeoArrowType::from_extension_field(field)
            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?
        {
            let nulls = array.logical_nulls();
            let encoder: Box<dyn Encoder> = match geoarrow_type {
                GeoArrowType::Point(typ) => Box::new(PointEncoder::new((array, typ).try_into()?)),
                GeoArrowType::LineString(typ) => {
                    Box::new(LineStringEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::Polygon(typ) => {
                    Box::new(PolygonEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::MultiPoint(typ) => {
                    Box::new(MultiPointEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::MultiLineString(typ) => {
                    Box::new(MultiLineStringEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::MultiPolygon(typ) => {
                    Box::new(MultiPolygonEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::Geometry(typ) => {
                    Box::new(GeometryEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::GeometryCollection(typ) => {
                    Box::new(GeometryCollectionEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::Wkb(typ) => {
                    Box::new(GenericWkbEncoder::<i32>::new((array, typ).try_into()?))
                }
                GeoArrowType::LargeWkb(typ) => {
                    Box::new(GenericWkbEncoder::<i64>::new((array, typ).try_into()?))
                }
                GeoArrowType::WkbView(typ) => {
                    Box::new(WkbViewEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::Wkt(typ) => {
                    Box::new(GenericWktEncoder::<i32>::new((array, typ).try_into()?))
                }
                GeoArrowType::LargeWkt(typ) => {
                    Box::new(GenericWktEncoder::<i64>::new((array, typ).try_into()?))
                }
                GeoArrowType::WktView(typ) => {
                    Box::new(WktViewEncoder::new((array, typ).try_into()?))
                }
                GeoArrowType::Rect(typ) => Box::new(RectEncoder::new((array, typ).try_into()?)),
            };
            Ok(Some(NullableEncoder::new(encoder, nulls)))
        } else {
            Ok(None)
        }
    }
}
