use crate::GeometryArrayTrait;
use crate::PointArray;
use geozero::{GeomProcessor, GeozeroGeometry};

impl GeozeroGeometry for PointArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for idx in 0..num_geometries {
            processor.point_begin(idx)?;
            processor.xy(self.coords.get_x(idx), self.coords.get_y(idx), 0)?;
            processor.point_end(idx)?;
        }

        processor.geometrycollection_end(num_geometries)?;
        Ok(())
    }
}
