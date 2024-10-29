use geo::CoordNum;

use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

/// Convert any coordinate to a [`geo::Coord`].
///
/// Only the first two dimensions will be kept.
pub fn coord_to_geo<T: CoordNum>(coord: &impl CoordTrait<T = T>) -> geo::Coord<T> {
    geo::Coord {
        x: coord.x(),
        y: coord.y(),
    }
}

/// Convert any Point to a [`geo::Point`].
///
/// Only the first two dimensions will be kept.
pub fn point_to_geo<T: CoordNum>(point: &impl PointTrait<T = T>) -> geo::Point<T> {
    if let Some(coord) = point.coord() {
        geo::Point(coord_to_geo(&coord))
    } else {
        todo!("converting empty point to geo not implemented")
    }
}

/// Convert any LineString to a [`geo::LineString`].
///
/// Only the first two dimensions will be kept.
pub fn line_string_to_geo<T: CoordNum>(
    line_string: &impl LineStringTrait<T = T>,
) -> geo::LineString<T> {
    geo::LineString::new(
        line_string
            .coords()
            .map(|coord| coord_to_geo(&coord))
            .collect(),
    )
}

/// Convert any Polygon to a [`geo::Polygon`].
///
/// Only the first two dimensions will be kept.
pub fn polygon_to_geo<T: CoordNum>(polygon: &impl PolygonTrait<T = T>) -> geo::Polygon<T> {
    let exterior = line_string_to_geo(&polygon.exterior().unwrap());
    let interiors = polygon
        .interiors()
        .map(|interior| line_string_to_geo(&interior))
        .collect();
    geo::Polygon::new(exterior, interiors)
}

/// Convert any MultiPoint to a [`geo::MultiPoint`].
///
/// Only the first two dimensions will be kept.
pub fn multi_point_to_geo<T: CoordNum>(
    multi_point: &impl MultiPointTrait<T = T>,
) -> geo::MultiPoint<T> {
    geo::MultiPoint::new(
        multi_point
            .points()
            .map(|point| point_to_geo(&point))
            .collect(),
    )
}

/// Convert any MultiLineString to a [`geo::MultiLineString`].
///
/// Only the first two dimensions will be kept.
pub fn multi_line_string_to_geo<T: CoordNum>(
    multi_line_string: &impl MultiLineStringTrait<T = T>,
) -> geo::MultiLineString<T> {
    geo::MultiLineString::new(
        multi_line_string
            .line_strings()
            .map(|line| line_string_to_geo(&line))
            .collect(),
    )
}

/// Convert any MultiPolygon to a [`geo::MultiPolygon`].
///
/// Only the first two dimensions will be kept.
pub fn multi_polygon_to_geo<T: CoordNum>(
    multi_polygon: &impl MultiPolygonTrait<T = T>,
) -> geo::MultiPolygon<T> {
    geo::MultiPolygon::new(
        multi_polygon
            .polygons()
            .map(|polygon| polygon_to_geo(&polygon))
            .collect(),
    )
}

/// Convert any Rect to a [`geo::Rect`].
///
/// Only the first two dimensions will be kept.
pub fn rect_to_geo<T: CoordNum>(rect: &impl RectTrait<T = T>) -> geo::Rect<T> {
    let c1 = coord_to_geo(&rect.min());
    let c2 = coord_to_geo(&rect.max());
    geo::Rect::new(c1, c2)
}

/// Convert any Geometry to a [`geo::Geometry`].
///
/// Only the first two dimensions will be kept.
pub fn geometry_to_geo<T: CoordNum>(geometry: &impl GeometryTrait<T = T>) -> geo::Geometry<T> {
    use GeometryType::*;

    match geometry.as_type() {
        Point(geom) => geo::Geometry::Point(point_to_geo(geom)),
        LineString(geom) => geo::Geometry::LineString(line_string_to_geo(geom)),
        Polygon(geom) => geo::Geometry::Polygon(polygon_to_geo(geom)),
        MultiPoint(geom) => geo::Geometry::MultiPoint(multi_point_to_geo(geom)),
        MultiLineString(geom) => geo::Geometry::MultiLineString(multi_line_string_to_geo(geom)),
        MultiPolygon(geom) => geo::Geometry::MultiPolygon(multi_polygon_to_geo(geom)),
        GeometryCollection(geom) => {
            geo::Geometry::GeometryCollection(geometry_collection_to_geo(geom))
        }
        Rect(geom) => geo::Geometry::Rect(rect_to_geo(geom)),
        Line(_) | Triangle(_) => todo!(),
    }
}

/// Convert any GeometryCollection to a [`geo::GeometryCollection`].
///
/// Only the first two dimensions will be kept.
pub fn geometry_collection_to_geo<T: CoordNum>(
    geometry_collection: &impl GeometryCollectionTrait<T = T>,
) -> geo::GeometryCollection<T> {
    geo::GeometryCollection::new_from(
        geometry_collection
            .geometries()
            .map(|geometry| geometry_to_geo(&geometry))
            .collect(),
    )
}
