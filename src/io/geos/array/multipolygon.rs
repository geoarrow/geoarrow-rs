use arrow_array::OffsetSizeTrait;

use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::{MultiPolygonArray, MultiPolygonBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::{GEOSConstPolygon, GEOSMultiPolygon, GEOSPolygon};
use geos::Geom;

// NOTE: this, `first_pass`, and `second_pass` are copied from their main implementations, because
// implementing geometry access traits on GEOS geometries that yield ConstGeometry objects with two
// lifetimes seemed really, really hard. Ideally one day we can unify the two branches!

impl<O: OffsetSizeTrait> MultiPolygonBuilder<O> {
    /// Add a new GEOS Polygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[allow(dead_code)]
    fn push_geos_polygon(&mut self, value: Option<&GEOSPolygon>) -> Result<()> {
        if let Some(polygon) = value {
            let exterior_ring = polygon.exterior();
            if exterior_ring.is_none() {
                self.push_empty();
                return Ok(());
            }

            // Total number of polygons in this MultiPolygon
            let num_polygons = 1;
            self.geom_offsets.try_push_usize(num_polygons).unwrap();

            let ext_ring = polygon.exterior().unwrap();
            let coord_seq = ext_ring.0.get_coord_seq()?;
            for coord_idx in 0..ext_ring.num_coords() {
                self.coords
                    .push_xy(coord_seq.get_x(coord_idx)?, coord_seq.get_y(coord_idx)?);
            }

            // Total number of rings in this Multipolygon
            self.polygon_offsets
                .try_push_usize(polygon.num_interiors() + 1)
                .unwrap();

            // Number of coords for each ring
            self.ring_offsets
                .try_push_usize(ext_ring.num_coords())
                .unwrap();

            for int_ring_idx in 0..polygon.num_interiors() {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                self.ring_offsets
                    .try_push_usize(int_ring.num_coords())
                    .unwrap();
                let coord_seq = int_ring.0.get_coord_seq()?;

                for coord_idx in 0..int_ring.num_coords() {
                    self.coords
                        .push_xy(coord_seq.get_x(coord_idx)?, coord_seq.get_y(coord_idx)?);
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Add a new GEOS MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    fn push_geos_multi_polygon(&mut self, value: Option<&GEOSMultiPolygon>) -> Result<()> {
        if let Some(multi_polygon) = value {
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            self.geom_offsets.try_push_usize(num_polygons).unwrap();

            // Iterate over polygons
            for polygon_idx in 0..num_polygons {
                let polygon = multi_polygon.polygon(polygon_idx).unwrap();

                // Here we unwrap the exterior ring because a polygon inside a multi polygon should
                // never be empty.
                let ext_ring = polygon.exterior().unwrap();
                let coord_seq = ext_ring.0.get_coord_seq()?;
                for coord_idx in 0..ext_ring.num_coords() {
                    self.coords
                        .push_xy(coord_seq.get_x(coord_idx)?, coord_seq.get_y(coord_idx)?);
                }

                // Total number of rings in this Multipolygon
                self.polygon_offsets
                    .try_push_usize(polygon.num_interiors() + 1)
                    .unwrap();

                // Number of coords for each ring
                self.ring_offsets
                    .try_push_usize(ext_ring.num_coords())
                    .unwrap();

                for int_ring_idx in 0..polygon.num_interiors() {
                    let int_ring = polygon.interior(int_ring_idx).unwrap();
                    self.ring_offsets
                        .try_push_usize(int_ring.num_coords())
                        .unwrap();
                    let coord_seq = int_ring.0.get_coord_seq()?;

                    for coord_idx in 0..int_ring.num_coords() {
                        self.coords
                            .push_xy(coord_seq.get_x(coord_idx)?, coord_seq.get_y(coord_idx)?);
                    }
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }
}

fn first_pass(
    geoms: &[Option<GEOSMultiPolygon>],
    geoms_length: usize,
) -> (usize, usize, usize, usize) {
    let mut coord_capacity = 0;
    let mut ring_capacity = 0;
    let mut polygon_capacity = 0;
    let geom_capacity = geoms_length;

    for multi_polygon in geoms.iter().flatten() {
        // Total number of polygons in this MultiPolygon
        let num_polygons = multi_polygon.num_polygons();
        polygon_capacity += num_polygons;

        for polygon_idx in 0..num_polygons {
            let polygon = GEOSConstPolygon::new_unchecked(
                multi_polygon.0.get_geometry_n(polygon_idx).unwrap(),
            );

            // Total number of rings in this MultiPolygon
            ring_capacity += polygon.num_interiors() + 1;

            // Number of coords for each ring
            if let Some(exterior) = polygon.exterior() {
                coord_capacity += exterior.num_coords();
            }

            for int_ring_idx in 0..polygon.num_interiors() {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                coord_capacity += int_ring.num_coords();
            }
        }
    }

    (
        coord_capacity,
        ring_capacity,
        polygon_capacity,
        geom_capacity,
    )
}

fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<GEOSMultiPolygon<'a>>>,
    coord_capacity: usize,
    ring_capacity: usize,
    polygon_capacity: usize,
    geom_capacity: usize,
) -> MultiPolygonBuilder<O> {
    let capacity = MultiPolygonCapacity::new(
        coord_capacity,
        ring_capacity,
        polygon_capacity,
        geom_capacity,
    );
    let mut array = MultiPolygonBuilder::with_capacity(capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_polygon| {
            array.push_geos_multi_polygon(maybe_multi_polygon.as_ref())
        })
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MultiPolygonBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiPolygon>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPolygon::new_unchecked))
            .collect();

        let (coord_capacity, ring_capacity, polygon_capacity, geom_capacity) =
            first_pass(&geos_objects, length);
        Ok(second_pass(
            geos_objects.into_iter(),
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
        ))
    }
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MultiPolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let mutable_arr: MultiPolygonBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipolygon::mp_array;

    #[test]
    fn geos_round_trip() {
        let arr = mp_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr.iter_geos().collect();
        let round_trip: MultiPolygonArray<i32> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
