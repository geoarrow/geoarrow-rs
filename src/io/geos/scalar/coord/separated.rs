use crate::array::SeparatedCoordBuffer;
use geos::CoordSeq;

impl TryFrom<SeparatedCoordBuffer<2>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: SeparatedCoordBuffer<2>) -> std::result::Result<Self, geos::Error> {
        CoordSeq::new_from_arrays(&value.buffers[0], &value.buffers[1], None, None)
    }
}
