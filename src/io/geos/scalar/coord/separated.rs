use crate::array::SeparatedCoordBuffer;
use crate::error::{GeoArrowError, Result};
use geos::CoordSeq;

impl TryFrom<SeparatedCoordBuffer> for CoordSeq<'_> {
    type Error = GeoArrowError;

    fn try_from(value: SeparatedCoordBuffer) -> Result<Self> {
        Ok(CoordSeq::new_from_arrays(&value.x, &value.y, None, None)?)
    }
}
