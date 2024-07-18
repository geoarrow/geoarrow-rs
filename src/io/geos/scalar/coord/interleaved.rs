use crate::array::InterleavedCoordBuffer;
use crate::GeometryArrayTrait;
use geos::CoordSeq;

impl TryFrom<InterleavedCoordBuffer<2>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: InterleavedCoordBuffer<2>) -> std::result::Result<Self, geos::Error> {
        CoordSeq::new_from_buffer(&value.coords, value.len(), false, false)
    }
}
