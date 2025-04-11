use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::MultiPolygonArray;
use crate::geozero::export::scalar::process_multi_polygon;
use crate::{ArrayAccessor, GeoArrowArray};

impl GeozeroGeometry for MultiPolygonArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_multi_polygon(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}
