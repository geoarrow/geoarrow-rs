use crate::array::CoordBuffer;
use geos::CoordSeq;

impl TryFrom<CoordBuffer<2>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: CoordBuffer<2>) -> std::result::Result<Self, geos::Error> {
        match value {
            CoordBuffer::Separated(cb) => cb.try_into(),
            CoordBuffer::Interleaved(cb) => cb.try_into(),
        }
    }
}
