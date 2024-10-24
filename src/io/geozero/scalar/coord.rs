use crate::geo_traits::CoordTrait;
use geozero::GeomProcessor;

pub(crate) fn process_coord<P: GeomProcessor>(
    coord: &impl CoordTrait<T = f64>,
    coord_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    use crate::geo_traits::Dimensions;

    match coord.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => processor.xy(coord.x(), coord.y(), coord_idx)?,
        Dimensions::Xyz | Dimensions::Unknown(3) => processor.coordinate(
            coord.x(),
            coord.y(),
            Some(coord.nth_unchecked(2)),
            None,
            None,
            None,
            coord_idx,
        )?,
        Dimensions::Xym => processor.coordinate(
            coord.x(),
            coord.y(),
            None,
            Some(coord.nth_unchecked(2)),
            None,
            None,
            coord_idx,
        )?,
        Dimensions::Xyzm | Dimensions::Unknown(4) => processor.coordinate(
            coord.x(),
            coord.y(),
            Some(coord.nth_unchecked(2)),
            Some(coord.nth_unchecked(3)),
            None,
            None,
            coord_idx,
        )?,
        d => panic!("Unexpected dimension {:?}", d),
    };
    Ok(())
}
