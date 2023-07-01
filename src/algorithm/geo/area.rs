use crate::error::Result;
use crate::{GeometryArray, GeometryArrayTrait};
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use geo::prelude::Area;

pub fn area(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
    }

    Ok(output_array.into())
}

pub fn signed_area(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        }
    }

    Ok(output_array.into())
}
