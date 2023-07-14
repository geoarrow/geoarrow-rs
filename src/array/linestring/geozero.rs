use arrow2::types::Offset;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::LineStringArray;
use crate::GeometryArrayTrait;

impl<O: Offset> GeozeroGeometry for LineStringArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let (start_coord_idx, end_coord_idx) = self.geom_offsets.start_end(geom_idx);

            processor.linestring_begin(true, end_coord_idx - start_coord_idx, geom_idx)?;

            for coord_idx in start_coord_idx..end_coord_idx {
                processor.xy(
                    self.coords.get_x(coord_idx),
                    self.coords.get_y(coord_idx),
                    coord_idx - start_coord_idx,
                )?;
            }

            processor.linestring_end(true, geom_idx)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geo::{line_string, LineString};
    use geozero::ToWkt;

    fn ls0() -> LineString {
        line_string![
            (x: 0., y: 1.),
            (x: 1., y: 2.)
        ]
    }

    fn ls1() -> LineString {
        line_string![
            (x: 3., y: 4.),
            (x: 5., y: 6.)
        ]
    }

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: LineStringArray = vec![ls0(), ls1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(LINESTRING(0 1,1 2),LINESTRING(3 4,5 6))";
        assert_eq!(wkt, expected);
        Ok(())
    }
}
