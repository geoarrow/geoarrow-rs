use crate::array::InterleavedCoordBuffer;
use crate::datatypes::Dimension;
use geos::CoordSeq;

impl TryFrom<InterleavedCoordBuffer> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: InterleavedCoordBuffer) -> std::result::Result<Self, geos::Error> {
        match value.dim {
            Dimension::XY => CoordSeq::new_from_buffer(&value.coords, value.len(), false, false),
            Dimension::XYZ => CoordSeq::new_from_buffer(&value.coords, value.len(), true, false),
        }
    }
}
