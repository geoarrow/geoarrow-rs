use crate::geo_traits::{MultiPointTrait, PointTrait};
use crate::scalar::MultiPoint;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_multi_point<P: GeomProcessor>(
    geom: &impl MultiPointTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.multipoint_begin(geom.num_points(), geom_idx)?;

    for (point_idx, point) in geom.points().enumerate() {
        processor.xy(point.x(), point.y(), point_idx)?;
    }

    processor.multipoint_end(geom_idx)?;
    Ok(())
}

impl<O: OffsetSizeTrait> GeozeroGeometry for MultiPoint<'_, O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_multi_point(self, 0, processor)
    }
}
