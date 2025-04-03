use crate::array::SeparatedCoordBuffer;
use geoarrow_schema::Dimension;
use geos::CoordSeq;

impl TryFrom<SeparatedCoordBuffer> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: SeparatedCoordBuffer) -> std::result::Result<Self, geos::Error> {
        match value.dim {
            Dimension::XY => {
                CoordSeq::new_from_arrays(&value.buffers[0], &value.buffers[1], None, None)
            }
            Dimension::XYZ => CoordSeq::new_from_arrays(
                &value.buffers[0],
                &value.buffers[1],
                Some(&value.buffers[2]),
                None,
            ),
            _ => todo!("XYM and XYZM not supported yet"),
        }
    }
}
