use crate::array::{GeometryArray, MutablePointArray, PointArray};
use crate::error::Result;
use crate::GeometryArrayTrait;
use geo::BoundingRect;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of that box
pub fn center(array: &GeometryArray) -> Result<PointArray> {
    let mut output_array = MutablePointArray::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(
                    maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                )
            });
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(maybe_g.map(|g| g.bounding_rect().center().into()))
            });
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(
                    maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                )
            });
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(
                    maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                )
            });
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(
                    maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                )
            });
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(
                    maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                )
            });
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push_geo(
                    maybe_g.and_then(|g| g.bounding_rect().map(|rect| rect.center().into())),
                )
            });
        }
    }

    Ok(output_array.into())
}
