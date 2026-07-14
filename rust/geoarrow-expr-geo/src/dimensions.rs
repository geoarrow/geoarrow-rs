use arrow_array::builder::{BooleanBuilder, Int32Builder};
use arrow_array::{BooleanArray, Int32Array};
use geo::dimensions::{Dimensions, HasDimensions};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::to_geo::geometry_to_geo;

/// Return the topological dimension of each geometry: `-1` for empty, `0` for
/// point-like, `1` for line-like, `2` for area-like.
///
/// `geo` measures each instance rather than its type, so a `LineString` of
/// identical coordinates reports `0`. A `GeometryCollection` reports the largest
/// dimension of its members.
pub fn dimensions(array: &dyn GeoArrowArray) -> GeoArrowResult<Int32Array> {
    downcast_geoarrow_array!(array, dimensions_impl)
}

/// Return whether each geometry is empty.
///
/// A `GeometryCollection` is empty only when it holds no members, even if every
/// member is itself empty. This follows `geo`'s `Geometry::is_empty`.
pub fn is_empty(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, is_empty_impl)
}

fn dimensions_to_i32(d: Dimensions) -> i32 {
    match d {
        Dimensions::Empty => -1,
        Dimensions::ZeroDimensional => 0,
        Dimensions::OneDimensional => 1,
        Dimensions::TwoDimensional => 2,
    }
}

fn dimensions_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Int32Array> {
    let mut builder = Int32Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(dimensions_to_i32(geo_geom.dimensions()));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

fn is_empty_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(geo_geom.is_empty());
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use arrow_array::create_array;
    use geo::{Geometry, GeometryCollection, LineString, MultiPoint, point, polygon};
    use geoarrow_array::GeoArrowArray;

    use super::*;
    use crate::test_util::geometry_array;

    fn mixed_array() -> impl GeoArrowArray {
        geometry_array(vec![
            Some(Geometry::from(point!(x: 1., y: 2.))),
            Some(Geometry::LineString(LineString::from(vec![
                (0.0, 0.0),
                (1.0, 1.0),
            ]))),
            Some(Geometry::Polygon(
                polygon![(x: 0., y: 0.), (x: 0., y: 4.), (x: 4., y: 4.), (x: 4., y: 0.)],
            )),
            Some(Geometry::MultiPoint(MultiPoint::new(vec![]))),
            None,
        ])
    }

    #[test]
    fn mixed_geometries() {
        let arr = mixed_array();
        assert_eq!(
            &dimensions(&arr).unwrap(),
            create_array!(Int32, [Some(0), Some(1), Some(2), Some(-1), None]).as_ref()
        );
        assert_eq!(
            &is_empty(&arr).unwrap(),
            create_array!(
                Boolean,
                [Some(false), Some(false), Some(false), Some(true), None]
            )
            .as_ref()
        );
    }

    /// A collection reports the largest dimension of its members, and an empty
    /// one reports `-1`. A collection whose members are all empty also reports
    /// `-1`, but is not itself empty: `geo::Geometry::is_empty` counts members
    /// and does not recurse into them.
    #[test]
    fn geometry_collection_dimension_and_emptiness() {
        let gc = |members| {
            Some(Geometry::GeometryCollection(GeometryCollection::new_from(
                members,
            )))
        };
        let geoms = vec![
            gc(vec![
                Geometry::from(point!(x: 0., y: 0.)),
                Geometry::Polygon(
                    polygon![(x: 0., y: 0.), (x: 0., y: 1.), (x: 1., y: 1.), (x: 1., y: 0.)],
                ),
            ]),
            gc(vec![]),
            gc(vec![
                Geometry::MultiPoint(MultiPoint::new(vec![])),
                Geometry::LineString(LineString::new(vec![])),
            ]),
        ];
        let arr = geometry_array(geoms);

        let dims = dimensions(&arr).unwrap();
        assert_eq!(
            &dims,
            create_array!(Int32, [Some(2), Some(-1), Some(-1)]).as_ref()
        );

        let empty = is_empty(&arr).unwrap();
        assert_eq!(
            &empty,
            create_array!(Boolean, [Some(false), Some(true), Some(false)]).as_ref()
        );
    }
}
