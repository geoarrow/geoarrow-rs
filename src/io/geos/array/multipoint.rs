use arrow2::types::Offset;
use geos::Geom;

use crate::array::{MultiPointArray, MutableMultiPointArray};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSMultiPoint;

// NOTE: this, `first_pass`, and `second_pass` are copied from their main implementations, because
// implementing geometry access traits on GEOS geometries that yield ConstGeometry objects with two
// lifetimes seemed really, really hard. Ideally one day we can unify the two branches!

impl<O: Offset> MutableMultiPointArray<O> {
    /// Push a GEOS multi point
    fn push_geos_multi_point(&mut self, value: Option<&GEOSMultiPoint>) -> Result<()> {
        if let Some(multi_point) = value {
            let num_points = multi_point.num_points();
            for point_idx in 0..num_points {
                let point = multi_point.0.get_geometry_n(point_idx).unwrap();
                let x = point.get_x()?;
                let y = point.get_y()?;
                unsafe {
                    self.push_xy(x, y)?;
                }
            }
            self.try_push_length(num_points)?;
        } else {
            self.push_null();
        }
        Ok(())
    }
}

fn first_pass(geoms: &[Option<GEOSMultiPoint>], geoms_length: usize) -> (usize, usize) {
    let mut coord_capacity = 0;
    let geom_capacity = geoms_length;

    for multi_point in geoms.iter().flatten() {
        coord_capacity += multi_point.num_points();
    }

    (coord_capacity, geom_capacity)
}

fn second_pass<'a, O: Offset>(
    geoms: impl Iterator<Item = Option<GEOSMultiPoint<'a>>>,
    coord_capacity: usize,
    geom_capacity: usize,
) -> MutableMultiPointArray<O> {
    let mut array = MutableMultiPointArray::with_capacities(coord_capacity, geom_capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_point| array.push_geos_multi_point(maybe_multi_point.as_ref()))
        .unwrap();

    array
}

impl<'a, O: Offset> TryFrom<Vec<Option<geos::Geometry<'a>>>> for MutableMultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_linestring_objects: Vec<Option<GEOSMultiPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPoint::new_unchecked))
            .collect();
        let (coord_capacity, geom_capacity) = first_pass(&geos_linestring_objects, length);
        Ok(second_pass(
            geos_linestring_objects.into_iter(),
            coord_capacity,
            geom_capacity,
        ))
    }
}

impl<'a, O: Offset> TryFrom<Vec<Option<geos::Geometry<'a>>>> for MultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: MutableMultiPointArray<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}
