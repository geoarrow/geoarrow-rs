use arrow2::types::Offset;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::array::MultiPolygonArray;
use crate::GeometryArrayTrait;

impl<O: Offset> GeozeroGeometry for MultiPolygonArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let (start_polygon_idx, end_polygon_idx) = self.geom_offsets.start_end(geom_idx);

            processor.multipolygon_begin(end_polygon_idx - start_polygon_idx, geom_idx)?;

            for polygon_idx in start_polygon_idx..end_polygon_idx {
                let (start_ring_idx, end_ring_idx) = self.polygon_offsets.start_end(polygon_idx);

                processor.polygon_begin(
                    false,
                    end_ring_idx - start_ring_idx,
                    polygon_idx - start_polygon_idx,
                )?;

                for ring_idx in start_ring_idx..end_ring_idx {
                    let (start_coord_idx, end_coord_idx) = self.ring_offsets.start_end(ring_idx);

                    processor.linestring_begin(
                        false,
                        end_coord_idx - start_coord_idx,
                        ring_idx - start_ring_idx,
                    )?;

                    for coord_idx in start_coord_idx..end_coord_idx {
                        processor.xy(
                            self.coords.get_x(coord_idx),
                            self.coords.get_y(coord_idx),
                            coord_idx - start_coord_idx,
                        )?;
                    }

                    processor.linestring_end(false, ring_idx - start_ring_idx)?;
                }

                processor.polygon_end(false, polygon_idx - start_polygon_idx)?;
            }

            processor.multipolygon_end(geom_idx)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::test::multipolygon::{mp0, mp1};

    use super::*;
    use geozero::ToWkt;

    #[test]
    fn geozero_process_geom() -> geozero::error::Result<()> {
        let arr: MultiPolygonArray<i64> = vec![mp0(), mp1()].into();
        let wkt = arr.to_wkt()?;
        let expected = "GEOMETRYCOLLECTION(MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44))),MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-110 44,-110 42,-105 42,-105 44,-110 44))))";
        assert_eq!(wkt, expected);
        Ok(())
    }
}
