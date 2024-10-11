use crate::geo_traits::{LineStringTrait, MultiLineStringTrait};
use crate::io::geozero::scalar::process_coord;
use crate::scalar::MultiLineString;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_multi_line_string<P: GeomProcessor>(
    geom: &impl MultiLineStringTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.multilinestring_begin(geom.num_line_strings(), geom_idx)?;

    for (line_idx, line) in geom.line_strings().enumerate() {
        processor.linestring_begin(false, line.num_points(), line_idx)?;

        for (coord_idx, coord) in line.points().enumerate() {
            process_coord(&coord, coord_idx, processor)?;
        }

        processor.linestring_end(false, line_idx)?;
    }

    processor.multilinestring_end(geom_idx)?;
    Ok(())
}

impl<const D: usize> GeozeroGeometry for MultiLineString<'_, D> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_multi_line_string(self, 0, processor)
    }
}
