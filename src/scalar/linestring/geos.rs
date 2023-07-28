use crate::error::{GeoArrowError, Result};
use crate::scalar::LineString;
use crate::GeometryArrayTrait;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<LineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: LineString<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a LineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a LineString<'_, O>) -> Result<geos::Geometry<'b>> {
        let (start, end) = value.geom_offsets.start_end(value.geom_index);

        let mut sliced_coords = value.coords.clone();
        sliced_coords.slice(start, end - start);

        Ok(geos::Geometry::create_line_string(
            sliced_coords.try_into()?,
        )?)
    }
}

impl<'b, O: Offset> LineString<'_, O> {
    pub fn to_geos_linear_ring(&self) -> Result<geos::Geometry<'b>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);

        let mut sliced_coords = self.coords.clone();
        sliced_coords.slice(start, end - start);

        Ok(geos::Geometry::create_linear_ring(
            sliced_coords.try_into()?,
        )?)
    }
}
