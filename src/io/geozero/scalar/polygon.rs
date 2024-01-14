use crate::geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use crate::scalar::Polygon;
use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

fn process_ring<P: GeomProcessor>(
    ring: impl LineStringTrait<T = f64>,
    ring_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.linestring_begin(false, ring.num_coords(), ring_idx)?;

    for (coord_idx, coord) in ring.coords().enumerate() {
        processor.xy(coord.x(), coord.y(), coord_idx)?;
    }

    processor.linestring_end(false, ring_idx)?;
    Ok(())
}

pub(crate) fn process_polygon<P: GeomProcessor>(
    geom: &impl PolygonTrait<T = f64>,
    tagged: bool,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.polygon_begin(tagged, geom.num_interiors() + 1, geom_idx)?;

    if let Some(exterior) = geom.exterior() {
        process_ring(exterior, 0, processor)?;
    }

    for (interior_ring_idx, interior_ring) in geom.interiors().enumerate() {
        process_ring(interior_ring, interior_ring_idx + 1, processor)?;
    }

    processor.polygon_end(tagged, geom_idx)?;

    Ok(())
}

impl<O: OffsetSizeTrait> GeozeroGeometry for Polygon<'_, O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_polygon(self, true, 0, processor)
    }
}
