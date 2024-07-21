use crate::geo_traits::CoordTrait;
use geozero::GeomProcessor;

pub(crate) fn process_coord<P: GeomProcessor>(
    coord: &impl CoordTrait<T = f64>,
    coord_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    match coord.dim() {
        2 => processor.xy(coord.x(), coord.y(), coord_idx)?,
        3 => processor.coordinate(
            coord.x(),
            coord.y(),
            Some(coord.nth_unchecked(2)),
            None,
            None,
            None,
            coord_idx,
        )?,
        _ => panic!(),
    };
    Ok(())
}
