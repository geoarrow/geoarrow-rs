use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::MultiPointArray;
use crate::geozero::export::scalar::process_multi_point;
use crate::{ArrayAccessor, GeoArrowArray};

impl GeozeroGeometry for MultiPointArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_multi_point(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use geoarrow_schema::{CoordType, Dimension, MultiPointType};
    use geozero::ToWkt;
    use geozero::error::Result;

    use crate::builder::MultiPointBuilder;
    use crate::test::multipoint::{mp0, mp1};

    #[test]
    fn geozero_process_geom() -> Result<()> {
        let typ = MultiPointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
        let geo_arr = MultiPointBuilder::from_multi_points(&[&mp0(), &mp1()], typ).finish();
        let wkt = ToWkt::to_wkt(&geo_arr)?;
        let expected = "GEOMETRYCOLLECTION(MULTIPOINT(0 1,1 2),MULTIPOINT(3 4,5 6))";
        assert_eq!(wkt, expected);
        Ok(())
    }
}
