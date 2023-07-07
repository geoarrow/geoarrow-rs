use crate::algorithm::broadcasting::BroadcastableGeometry;
use crate::array::{GeometryArray, PointArray, LineStringArray};
use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType as ArrowDataType;
use geo::algorithm::EuclideanDistance;
use geo::Geometry;

pub fn euclidean_distance(
    array: GeometryArray,
    other: BroadcastableGeometry,
) -> Result<PrimitiveArray<f64>> {
    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_euclidean_length)));
        }
        GeometryArray::Point(arr) => {
            euclidean_distance_point(arr, other)
        }
        GeometryArray::LineString(arr) => {
            euclidean_distance_line_string(arr, other)
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().euclidean_length()))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| {
                    g.iter()
                        .map(|poly| poly.exterior().euclidean_length())
                        .sum()
                }))
            });
        }
    }
}

fn euclidean_distance_point(
    arr: PointArray,
    other: BroadcastableGeometry,
) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(arr.len());

    match other {
        BroadcastableGeometry::Point(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::LineString(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::Polygon(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::MultiPoint(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::MultiLineString(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::MultiPolygon(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
    };

    Ok(output_array.into())
}

fn euclidean_distance_line_string(
    arr: LineStringArray,
    other: BroadcastableGeometry,
) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(arr.len());

    match other {
        BroadcastableGeometry::Point(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::LineString(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::Polygon(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::MultiPoint(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::MultiLineString(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
        BroadcastableGeometry::MultiPolygon(other) => arr
            .iter_geo_values()
            .zip(other.into_iter())
            .for_each(|(g1, g2)| output_array.push(Some(g1.euclidean_distance(&g2)))),
    };

    Ok(output_array.into())
}
