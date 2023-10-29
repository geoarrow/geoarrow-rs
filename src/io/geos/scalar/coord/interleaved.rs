use crate::array::InterleavedCoordBuffer;
use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;
use geos::CoordSeq;

impl TryFrom<InterleavedCoordBuffer> for CoordSeq<'_> {
    type Error = GeoArrowError;

    fn try_from(value: InterleavedCoordBuffer) -> Result<Self> {
        Ok(CoordSeq::new_from_buffer(
            &value.coords,
            value.len(),
            false,
            false,
        )?)
    }
}
