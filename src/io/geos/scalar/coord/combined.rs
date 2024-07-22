use crate::array::CoordBuffer;
use geos::CoordSeq;

impl<const D: usize> TryFrom<CoordBuffer<D>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: CoordBuffer<D>) -> std::result::Result<Self, geos::Error> {
        match value {
            CoordBuffer::Separated(cb) => cb.try_into(),
            CoordBuffer::Interleaved(cb) => cb.try_into(),
        }
    }
}
