use crate::array::InterleavedCoordBuffer;
use crate::GeometryArrayTrait;
use geos::CoordSeq;

impl<const D: usize> TryFrom<InterleavedCoordBuffer<D>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: InterleavedCoordBuffer<D>) -> std::result::Result<Self, geos::Error> {
        CoordSeq::new_from_buffer(&value.coords, value.len(), false, false)
    }
}
