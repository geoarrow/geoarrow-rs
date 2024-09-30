use crate::geo_traits::LineStringTrait;
use crate::io::geozero::scalar::process_coord;
use crate::scalar::LineString;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_line_string<P: GeomProcessor>(
    geom: &impl LineStringTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.linestring_begin(true, geom.num_coords(), geom_idx)?;

    for (coord_idx, coord) in geom.coords().enumerate() {
        process_coord(&coord, coord_idx, processor)?;
    }

    processor.linestring_end(true, geom_idx)?;
    Ok(())
}

impl<const D: usize> GeozeroGeometry for LineString<'_, D> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_line_string(self, 0, processor)
    }
}
