use arrow_array::Float64Array;
use geo::ChamberlainDuquetteArea;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;

use crate::util::{map_to_f64, zeros};

/// Compute the signed approximate area, in square meters, of each geometry.
///
/// Chamberlain-Duquette is a spherical approximation on the WGS84 equatorial
/// radius, not the ellipsoidal geodesic area. Coordinates are interpreted as
/// lon/lat degrees, and the sign follows ring winding. Non-areal geometries have
/// zero area.
pub fn chamberlain_duquette_signed_area(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, signed_area_impl)
}

/// Compute the unsigned approximate area, in square meters, of each geometry.
/// See [`chamberlain_duquette_signed_area`].
pub fn chamberlain_duquette_unsigned_area(
    array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, unsigned_area_impl)
}

fn signed_area_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(zeros(array.len(), array.logical_nulls()))
        }
        _ => map_to_f64(array, |g| g.chamberlain_duquette_signed_area()),
    }
}

fn unsigned_area_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(zeros(array.len(), array.logical_nulls()))
        }
        _ => map_to_f64(array, |g| g.chamberlain_duquette_unsigned_area()),
    }
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use geo::{Geometry, LineString, Polygon, point, polygon};
    use geoarrow_array::builder::PolygonBuilder;
    use geoarrow_schema::{CoordType, Dimension, PolygonType};

    use super::*;
    use crate::test_util::geometry_array;

    /// The O2 in London, from `geo`'s `ChamberlainDuquetteArea` doctest.
    fn o2() -> Polygon {
        polygon![
            (x: 0.00388383, y: 51.501574),
            (x: 0.00538587, y: 51.502278),
            (x: 0.00553607, y: 51.503299),
            (x: 0.00467777, y: 51.504181),
            (x: 0.00327229, y: 51.504435),
            (x: 0.00187754, y: 51.504168),
            (x: 0.00087976, y: 51.503380),
            (x: 0.00107288, y: 51.502324),
            (x: 0.00185608, y: 51.501770),
            (x: 0.00388383, y: 51.501574),
        ]
    }

    #[test]
    fn area_polygon() {
        let typ = PolygonType::new(Dimension::XY, Default::default())
            .with_coord_type(CoordType::Interleaved);
        let mut builder = PolygonBuilder::new(typ);
        let poly = o2();
        builder.push_polygon(Some(&poly)).unwrap();
        let arr = builder.finish();

        let signed = chamberlain_duquette_signed_area(&arr).unwrap();
        let unsigned = chamberlain_duquette_unsigned_area(&arr).unwrap();

        assert_eq!(signed.value(0), poly.chamberlain_duquette_signed_area());
        assert_eq!(unsigned.value(0), poly.chamberlain_duquette_unsigned_area());
    }

    #[test]
    fn area_zero() {
        let geoms = vec![
            Some(Geometry::from(point!(x: 0., y: 51.5))),
            Some(Geometry::LineString(LineString::from(vec![
                (0.0, 0.0),
                (1.0, 1.0),
            ]))),
            Some(Geometry::Polygon(o2())),
            None,
        ];
        let arr = geometry_array(geoms);

        let unsigned = chamberlain_duquette_unsigned_area(&arr).unwrap();
        assert_eq!(unsigned.value(0), 0.0);
        assert_eq!(unsigned.value(1), 0.0);
        assert_eq!(unsigned.value(2), o2().chamberlain_duquette_unsigned_area());
        assert!(unsigned.is_null(3));
    }
}
