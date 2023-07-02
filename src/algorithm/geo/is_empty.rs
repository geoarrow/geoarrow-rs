use crate::array::GeometryArray;
use crate::error::Result;
use crate::GeometryArrayTrait;
use arrow2::array::{BooleanArray, MutableBooleanArray};
use geo::dimensions::HasDimensions;

pub fn is_empty(array: GeometryArray) -> Result<BooleanArray> {
    let mut output_array = MutableBooleanArray::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
    }

    Ok(output_array.into())
}
