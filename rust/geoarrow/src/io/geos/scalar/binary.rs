use crate::scalar::WKB;
use arrow_array::OffsetSizeTrait;

impl<'a, O: OffsetSizeTrait> TryFrom<&'a WKB<O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a WKB<O>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::new_from_wkb(value.as_ref())
    }
}
