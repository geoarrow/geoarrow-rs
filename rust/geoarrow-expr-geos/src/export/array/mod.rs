use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::export::to_geos_geometry;

/// Convert a GeoArrow array into GEOS geometries.
///
/// Null elements are preserved as `None`.
pub trait ToGEOS {
    /// Convert each element of the array into a GEOS geometry, preserving nulls.
    ///
    /// # Errors
    ///
    /// Inputs are expected to be standard geometry types.
    /// `Rect`, `Triangle`, and `Line` are not supported.
    fn to_geos(&self) -> GeoArrowResult<Vec<Option<geos::Geometry>>>;
}

impl ToGEOS for dyn GeoArrowArray + '_ {
    fn to_geos(&self) -> GeoArrowResult<Vec<Option<geos::Geometry>>> {
        downcast_geoarrow_array!(self, impl_to_geos)
    }
}

fn impl_to_geos<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Vec<Option<geos::Geometry>>> {
    array
        .iter()
        .map(|item| match item {
            None => Ok(None),
            Some(geom) => Ok(Some(
                to_geos_geometry(&geom?).map_err(|err| GeoArrowError::External(Box::new(err)))?,
            )),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use geoarrow_array::array::PointArray;
    use geoarrow_array::test::point::array;
    use geoarrow_array::{GeoArrowArray, IntoArrow};
    use geoarrow_schema::{CoordType, Dimension};

    use super::ToGEOS;
    use crate::import::array::FromGEOS;

    #[test]
    fn to_geos_round_trip() {
        // M-dimension support requires GEOS 3.14
        #[cfg(not(feature = "geos-3_14"))]
        let dims = [Dimension::XY, Dimension::XYZ];
        #[cfg(feature = "geos-3_14")]
        let dims = [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ];

        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in dims {
                let arr = array(coord_type, dim);

                let geos_geoms = (&arr as &dyn GeoArrowArray).to_geos().unwrap();
                let round_trip =
                    PointArray::from_geos(geos_geoms, arr.extension_type().clone()).unwrap();
                assert_eq!(arr, round_trip);
            }
        }
    }
}
