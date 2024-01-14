use crate::geo_traits::{CoordTrait, LineStringTrait, MultiLineStringTrait};
use crate::scalar::MultiLineString;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_multi_line_string<P: GeomProcessor>(
    geom: &impl MultiLineStringTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.multilinestring_begin(geom.num_lines(), geom_idx)?;

    for (line_idx, line) in geom.lines().enumerate() {
        processor.linestring_begin(false, line.num_coords(), line_idx)?;

        for (coord_idx, coord) in line.coords().enumerate() {
            processor.xy(coord.x(), coord.y(), coord_idx)?;
        }

        processor.linestring_end(false, line_idx)?;
    }

    processor.multilinestring_end(geom_idx)?;
    Ok(())
}

impl<O: OffsetSizeTrait> GeozeroGeometry for MultiLineString<'_, O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_multi_line_string(self, 0, processor)
    }
}
