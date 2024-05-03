use crate::scalar::WKB;
use arrow_array::OffsetSizeTrait;

impl<O: OffsetSizeTrait> TryFrom<WKB<'_, O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: WKB<'_, O>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<&'a WKB<'_, O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a WKB<'_, O>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::new_from_wkb(value.arr.value(value.geom_index))
    }
}
