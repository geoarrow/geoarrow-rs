use crate::array::CoordBuffer;
use crate::scalar::Coord;
use geo_traits::CoordTrait;
use geos::{CoordDimensions, CoordSeq};

impl<'a> TryFrom<&'a Coord<'_>> for geos::CoordSeq {
    type Error = geos::Error;

    fn try_from(coord: &'a Coord<'_>) -> std::result::Result<geos::CoordSeq, geos::Error> {
        coord_to_geos(coord)
    }
}

impl TryFrom<CoordBuffer> for CoordSeq {
    type Error = geos::Error;

    fn try_from(value: CoordBuffer) -> std::result::Result<Self, geos::Error> {
        match value {
            CoordBuffer::Separated(cb) => cb.try_into(),
            CoordBuffer::Interleaved(cb) => cb.try_into(),
        }
    }
}

pub(crate) fn coord_to_geos(
    coord: &impl CoordTrait<T = f64>,
) -> std::result::Result<geos::CoordSeq, geos::Error> {
    use geo_traits::Dimensions;

    match coord.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => {
            let mut coord_seq = CoordSeq::new(1, CoordDimensions::TwoD)?;
            coord_seq.set_x(0, coord.x())?;
            coord_seq.set_y(0, coord.y())?;
            Ok(coord_seq)
        }
        Dimensions::Xyz | Dimensions::Unknown(3) => {
            let mut coord_seq = CoordSeq::new(1, CoordDimensions::ThreeD)?;
            coord_seq.set_x(0, coord.x())?;
            coord_seq.set_y(0, coord.y())?;
            coord_seq.set_z(0, coord.nth(2).unwrap())?;
            Ok(coord_seq)
        }
        _ => Err(geos::Error::GenericError(
            "Unexpected dimension".to_string(),
        )),
    }
}

pub(crate) fn coords_to_geos<C: CoordTrait<T = f64>, I: ExactSizeIterator<Item = C>>(
    coords: I,
    dims: CoordDimensions,
) -> std::result::Result<geos::CoordSeq, geos::Error> {
    let mut coord_seq = CoordSeq::new(coords.len().try_into().unwrap(), dims)?;
    let is_3d = matches!(dims, CoordDimensions::ThreeD);

    coords.enumerate().try_for_each(|(idx, coord)| {
        coord_seq.set_x(idx, coord.nth_or_panic(0))?;
        coord_seq.set_y(idx, coord.nth_or_panic(1))?;

        if is_3d {
            coord_seq.set_z(idx, coord.nth_or_panic(2))?;
        }
        Ok(())
    })?;

    Ok(coord_seq)
}
