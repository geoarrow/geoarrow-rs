use crate::error::GeoArrowError;
use crate::geo_traits::MultiLineStringTrait;
use crate::scalar::MultiLineString;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<MultiLineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiLineString<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a MultiLineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a MultiLineString<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        let num_lines = value.num_lines();
        let mut geos_geoms = Vec::with_capacity(num_lines);

        for line_idx in 0..num_lines {
            let line = value.line(line_idx).unwrap();
            geos_geoms.push(line.try_into()?);
        }

        Ok(geos::Geometry::create_multiline_string(geos_geoms)?)
    }
}
