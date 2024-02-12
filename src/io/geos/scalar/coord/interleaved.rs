use crate::array::InterleavedCoordBuffer;
use crate::GeometryArrayTrait;
use geos::CoordSeq;

impl TryFrom<InterleavedCoordBuffer> for CoordSeq<'_> {
    type Error = geos::Error;

    fn try_from(value: InterleavedCoordBuffer) -> std::result::Result<Self, geos::Error> {
        CoordSeq::new_from_buffer(&value.coords, value.len(), false, false)
    }
}
