use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::PointArray;
use crate::geozero::export::scalar::process_point;
use crate::{ArrayAccessor, GeoArrowArray};

impl GeozeroGeometry for PointArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for idx in 0..num_geometries {
            process_point(&self.value(idx).unwrap(), idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries)?;
        Ok(())
    }
}
