use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<WKB<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: WKB<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a WKB<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a WKB<'_, O>) -> Result<geos::Geometry<'b>> {
        Ok(geos::Geometry::new_from_wkb(
            value.arr.value(value.geom_index),
        )?)
    }
}
