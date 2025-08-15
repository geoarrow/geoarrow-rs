use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::RectArray;
use crate::geozero::export::scalar::process_rect;
use crate::{GeoArrowArray, GeoArrowArrayAccessor};

impl GeozeroGeometry for RectArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_rect(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use geoarrow_schema::{BoxType, Dimension};
    use geozero::ToWkt;

    use crate::builder::RectBuilder;
    use crate::test::rect::{r0, r1};

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let typ = BoxType::new(Dimension::XY, Default::default());
        let geo_arr = RectBuilder::from_rects([r0(), r1()].iter(), typ).finish();
        let wkt = ToWkt::to_wkt(&geo_arr)?;
        let expected = "GEOMETRYCOLLECTION(POLYGON((10 10,10 20,30 20,30 10,10 10)),POLYGON((100 100,100 200,300 200,300 100,100 100)))";
        assert_eq!(wkt, expected);
        Ok(())
    }
}
