use crate::geo_traits::PointTrait;
use geozero::GeomProcessor;

pub(crate) fn process_coord<P: GeomProcessor>(
    coord: &impl PointTrait<T = f64>,
    coord_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    use crate::geo_traits::Dimension;

    match coord.dim() {
        Dimension::XY | Dimension::Unknown(2) => processor.xy(coord.x(), coord.y(), coord_idx)?,
        Dimension::XYZ | Dimension::Unknown(3) => processor.coordinate(
            coord.x(),
            coord.y(),
            Some(coord.nth_unchecked(2)),
            None,
            None,
            None,
            coord_idx,
        )?,
        Dimension::XYM => processor.coordinate(
            coord.x(),
            coord.y(),
            None,
            Some(coord.nth_unchecked(2)),
            None,
            None,
            coord_idx,
        )?,
        Dimension::XYZM | Dimension::Unknown(4) => processor.coordinate(
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
