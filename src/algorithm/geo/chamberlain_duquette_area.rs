use crate::error::Result;
use crate::{GeometryArray, GeometryArrayTrait};
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use geo::prelude::ChamberlainDuquetteArea;

/// Calculate the unsigned approximate geodesic area of geometries on a sphere using the algorithm
/// presented in Some Algorithms for Polygons on a Sphere by Chamberlain and Duquette (2007)
pub fn chamberlain_duquette_unsigned_area(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
            });
        }
    }

    Ok(output_array.into())
}

/// Calculate the signed approximate geodesic area of geometries on a sphere using the algorithm
/// presented in Some Algorithms for Polygons on a Sphere by Chamberlain and Duquette (2007)
pub fn chamberlain_duquette_signed_area(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
            });
        }
    }

    Ok(output_array.into())
}
