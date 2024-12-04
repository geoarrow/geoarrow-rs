use crate::array::MixedGeometryArray;
use crate::io::geozero::scalar::process_geometry;
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use geozero::{GeomProcessor, GeozeroGeometry};

impl GeozeroGeometry for MixedGeometryArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_geometry(&self.value(geom_idx), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}
