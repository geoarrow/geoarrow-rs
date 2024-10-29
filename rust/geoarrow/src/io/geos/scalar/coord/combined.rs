use crate::array::CoordBuffer;
use crate::scalar::Coord;
use geo_traits::CoordTrait;
use geos::{CoordDimensions, CoordSeq};

impl<'a, const D: usize> TryFrom<&'a Coord<'_, D>> for geos::CoordSeq {
    type Error = geos::Error;

    fn try_from(point: &'a Coord<'_, D>) -> std::result::Result<geos::CoordSeq, geos::Error> {
        use geo_traits::Dimensions;

        match point.dim() {
            Dimensions::Xy | Dimensions::Unknown(2) => {
                let mut coord_seq = CoordSeq::new(1, CoordDimensions::TwoD)?;
                coord_seq.set_x(0, point.x())?;
                coord_seq.set_y(0, point.y())?;
                Ok(coord_seq)
            }
            Dimensions::Xyz | Dimensions::Unknown(3) => {
                let mut coord_seq = CoordSeq::new(1, CoordDimensions::ThreeD)?;
                coord_seq.set_x(0, point.x())?;
                coord_seq.set_y(0, point.y())?;
                coord_seq.set_z(0, point.nth(2).unwrap())?;
                Ok(coord_seq)
            }
            _ => Err(geos::Error::GenericError(
                "Unexpected dimension".to_string(),
            )),
        }
    }
}

impl<const D: usize> TryFrom<CoordBuffer<D>> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: CoordBuffer<D>) -> std::result::Result<Self, geos::Error> {
        match value {
            CoordBuffer::Separated(cb) => cb.try_into(),
            CoordBuffer::Interleaved(cb) => cb.try_into(),
        }
    }
}
