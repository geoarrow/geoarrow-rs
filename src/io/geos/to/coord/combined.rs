use crate::array::CoordBuffer;
use crate::error::{GeoArrowError, Result};
use geos::CoordSeq;

impl TryFrom<CoordBuffer> for CoordSeq<'_> {
    type Error = GeoArrowError;

    fn try_from(value: CoordBuffer) -> Result<Self> {
        match value {
            CoordBuffer::Separated(cb) => cb.try_into(),
            CoordBuffer::Interleaved(cb) => cb.try_into(),
        }
    }
}
