use crate::algorithm::broadcasting::BroadcastableVec;
use crate::array::GeometryArray;
use crate::error::Result;
use geo::{AffineTransform, MapCoords};

/// Used to express the origin for a given transform. Can be specified either be with reference to
/// the geometry being transformed (Centroid, Center) or some arbitrary point.
///
/// - Centroid: Use the centriod of each geometry in the series as the transform origin.
/// - Center: Use the center of each geometry in the series as the transform origin. The center is
///   defined as the center of the bounding box of the geometry
/// - Point: Define a single point to transform each geometry in the series about.
pub enum TransformOrigin {
    Centroid,
    Center,
    Point(geo::Point),
}

/// Apply an affine transformation on an array of geometries
pub fn affine_transform(
    array: &GeometryArray,
    transform: BroadcastableVec<AffineTransform>,
) -> Result<GeometryArray> {
    match array {
        GeometryArray::WKB(arr) => {
            let output_geoms: Vec<Option<geo::Geometry>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::WKB(output_geoms.into()))
        }
        GeometryArray::Point(arr) => {
            let output_geoms: Vec<Option<geo::Point>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::Point(output_geoms.into()))
        }

        GeometryArray::MultiPoint(arr) => {
            let output_geoms: Vec<Option<geo::MultiPoint>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::MultiPoint(output_geoms.into()))
        }
        GeometryArray::LineString(arr) => {
            let output_geoms: Vec<Option<geo::LineString>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::LineString(output_geoms.into()))
        }
        GeometryArray::MultiLineString(arr) => {
            let output_geoms: Vec<Option<geo::MultiLineString>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::MultiLineString(output_geoms.into()))
        }
        GeometryArray::Polygon(arr) => {
            let output_geoms: Vec<Option<geo::Polygon>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<geo::MultiPolygon>> = arr
                .iter_geo()
                .zip(transform.into_iter())
                .map(|(maybe_g, transform)| {
                    maybe_g.map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                })
                .collect();

            Ok(GeometryArray::MultiPolygon(output_geoms.into()))
        }
    }
}
