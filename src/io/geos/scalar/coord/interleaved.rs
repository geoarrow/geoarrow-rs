use crate::array::InterleavedCoordBuffer;
use crate::GeometryArrayTrait;
use geos::CoordSeq;

impl<const D: usize> TryFrom<InterleavedCoordBuffer<D>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: InterleavedCoordBuffer<D>) -> std::result::Result<Self, geos::Error> {
        match D {
            2 => CoordSeq::new_from_buffer(&value.coords, value.len(), false, false),
            3 => CoordSeq::new_from_buffer(&value.coords, value.len(), true, false),
            _ => panic!(),
        }
    }
}
