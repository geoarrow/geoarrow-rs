use geo_traits::CoordTrait;
use geoarrow_array::array::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
use geoarrow_schema::Dimension;
use geos::{CoordDimensions, CoordSeq};

#[allow(dead_code)]
fn coord_buffer_to_geos(coords: &CoordBuffer) -> Result<geos::CoordSeq, geos::Error> {
    match coords {
        CoordBuffer::Separated(cb) => separated_coords_to_geos(cb),
        CoordBuffer::Interleaved(cb) => interleaved_coords_to_geos(cb),
    }
}

fn separated_coords_to_geos(coords: &SeparatedCoordBuffer) -> Result<geos::CoordSeq, geos::Error> {
    match coords.dim() {
        Dimension::XY => CoordSeq::new_from_arrays(
            &coords.raw_buffers()[0],
            &coords.raw_buffers()[1],
            None,
            None,
        ),
        Dimension::XYZ => CoordSeq::new_from_arrays(
            &coords.raw_buffers()[0],
            &coords.raw_buffers()[1],
            Some(&coords.raw_buffers()[2]),
            None,
        ),
        _ => todo!("XYM and XYZM not supported yet"),
    }
}

fn interleaved_coords_to_geos(
    coords: &InterleavedCoordBuffer,
) -> Result<geos::CoordSeq, geos::Error> {
    match coords.dim() {
        Dimension::XY => CoordSeq::new_from_buffer(coords.coords(), coords.len(), false, false),
        Dimension::XYZ => CoordSeq::new_from_buffer(coords.coords(), coords.len(), true, false),
        _ => todo!("XYM and XYZM not supported yet"),
    }
}

pub(crate) fn dims_to_geos(dim: geo_traits::Dimensions) -> geos::CoordDimensions {
    match dim {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {
            geos::CoordDimensions::TwoD
        }
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => {
            geos::CoordDimensions::ThreeD
        }
        _ => panic!("Invalid coord dimension for GEOS: {:?}", dim),
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
