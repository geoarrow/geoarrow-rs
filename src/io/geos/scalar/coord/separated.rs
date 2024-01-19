use crate::array::SeparatedCoordBuffer;
use geos::CoordSeq;

impl TryFrom<SeparatedCoordBuffer> for CoordSeq<'_> {
    type Error = geos::Error;

    fn try_from(value: SeparatedCoordBuffer) -> std::result::Result<Self, geos::Error> {
        CoordSeq::new_from_arrays(&value.x, &value.y, None, None)
    }
}
