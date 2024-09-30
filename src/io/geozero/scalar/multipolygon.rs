use crate::geo_traits::MultiPolygonTrait;
use crate::io::geozero::scalar::polygon::process_polygon;
use crate::scalar::MultiPolygon;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_multi_polygon<P: GeomProcessor>(
    geom: &impl MultiPolygonTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.multipolygon_begin(geom.num_polygons(), geom_idx)?;

    for (polygon_idx, polygon) in geom.polygons().enumerate() {
        process_polygon(&polygon, false, polygon_idx, processor)?;
    }

    processor.multipolygon_end(geom_idx)?;
    Ok(())
}

impl<const D: usize> GeozeroGeometry for MultiPolygon<'_, D> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_multi_polygon(self, 0, processor)
    }
}
