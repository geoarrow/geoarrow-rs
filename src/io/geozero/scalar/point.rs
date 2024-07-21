use crate::geo_traits::PointTrait;
use crate::scalar::Point;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_point<P: GeomProcessor>(
    geom: &impl PointTrait<2, T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.point_begin(geom_idx)?;
    processor.xy(geom.x(), geom.y(), 0)?;
    processor.point_end(geom_idx)?;
    Ok(())
}

impl GeozeroGeometry for Point<'_, 2> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_point(self, 0, processor)
    }
}
