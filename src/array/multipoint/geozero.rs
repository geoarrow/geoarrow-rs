use crate::array::MultiPointArray;
use crate::GeometryArrayTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

impl GeozeroGeometry for MultiPointArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let (start_coord_idx, end_coord_idx) = self.geom_offsets.start_end(geom_idx);

            processor.multipoint_begin(end_coord_idx - start_coord_idx, geom_idx)?;

            for coord_idx in start_coord_idx..end_coord_idx {
                processor.xy(
                    self.coords.get_x(coord_idx),
                    self.coords.get_y(coord_idx),
                    coord_idx - start_coord_idx,
                )?;
            }

            processor.multipoint_end(geom_idx)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipoint::{mp0, mp1};
    use geozero::ToWkt;

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: MultiPointArray = vec![mp0(), mp1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(MULTIPOINT(0 1,1 2),MULTIPOINT(3 4,5 6))";
        assert_eq!(wkt, expected);
        Ok(())
    }
}
