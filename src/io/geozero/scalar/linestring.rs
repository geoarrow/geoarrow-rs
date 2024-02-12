use crate::geo_traits::{CoordTrait, LineStringTrait};
use crate::scalar::LineString;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

pub(crate) fn process_line_string<P: GeomProcessor>(
    geom: &impl LineStringTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.linestring_begin(true, geom.num_coords(), geom_idx)?;

    for (coord_idx, coord) in geom.coords().enumerate() {
        processor.xy(coord.x(), coord.y(), coord_idx)?;
    }

    processor.linestring_end(true, geom_idx)?;
    Ok(())
}

impl<O: OffsetSizeTrait> GeozeroGeometry for LineString<'_, O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_line_string(self, 0, processor)
    }
}
