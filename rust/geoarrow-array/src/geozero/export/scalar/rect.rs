use geo_traits::RectTrait;
use geozero::GeomProcessor;

/// Process a [RectTrait] through a [GeomProcessor].
pub(crate) fn process_rect<P: GeomProcessor>(
    _geom: &impl RectTrait<T = f64>,
    _geom_idx: usize,
    _processor: &mut P,
) -> geozero::error::Result<()> {
    todo!("Implement process_rect");
    // processor.point_begin(geom_idx)?;
    // process_point_as_coord(geom, 0, processor)?;
    // processor.point_end(geom_idx)?;
    // Ok(())
}
