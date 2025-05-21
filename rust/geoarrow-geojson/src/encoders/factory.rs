use arrow_array::Array;
use arrow_json::writer::NullableEncoder;
use arrow_json::{EncoderFactory, EncoderOptions};
use arrow_schema::{ArrowError, FieldRef};
use geoarrow_schema::{LineStringType, PointType, PolygonType};

use crate::encoders::linestring::LineStringEncoder;
use crate::encoders::point::PointEncoder;
use crate::encoders::polygon::PolygonEncoder;

/// A [EncoderFactory] for writing GeoArrow arrays.
///
/// This is internal because its use is encapsulated within the constructors for GeoJsonWriter and
/// GeoJsonLinesWriter.
#[derive(Debug)]
pub(crate) struct GeometryEncoderFactory;

impl EncoderFactory for GeometryEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let nulls = array.nulls().cloned();
        if let Ok(typ) = field.try_extension_type::<PointType>() {
            let geom_arr = (array, typ)
                .try_into()
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
            let encoder = PointEncoder::new(geom_arr);
            Ok(Some(NullableEncoder::new(Box::new(encoder), nulls)))
        } else if let Ok(typ) = field.try_extension_type::<LineStringType>() {
            let geom_arr = (array, typ)
                .try_into()
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
            let encoder = LineStringEncoder::new(geom_arr);
            Ok(Some(NullableEncoder::new(Box::new(encoder), nulls)))
        } else if let Ok(typ) = field.try_extension_type::<PolygonType>() {
            let geom_arr = (array, typ)
                .try_into()
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
            let encoder = PolygonEncoder::new(geom_arr);
            Ok(Some(NullableEncoder::new(Box::new(encoder), nulls)))
        } else {
            Ok(None)
        }
    }
}
