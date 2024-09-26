#![allow(dead_code)]
use geo::CoordFloat;

use crate::geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

fn coord_to_wkt<T: CoordFloat>(coord: &impl CoordTrait<T = T>) -> wkt::types::Coord<T> {
    let mut out = wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z: None,
        m: None,
    };
    if coord.dim() == 3 {
        out.z = Some(coord.nth_unchecked(2));
    }
    out
}

fn point_to_wkt<T: CoordFloat>(point: &impl PointTrait<T = T>) -> wkt::types::Point<T> {
    if point.x().is_nan() && point.y().is_nan() {
        return wkt::types::Point(None);
    }

    let mut coord = wkt::types::Coord {
        x: point.x(),
        y: point.y(),
        z: None,
        m: None,
    };
    if point.dim() == 3 {
        coord.z = Some(point.nth_unchecked(2));
    }
    wkt::types::Point(Some(coord))
}

fn line_string_to_wkt<T: CoordFloat>(
    line_string: &impl LineStringTrait<T = T>,
) -> wkt::types::LineString<T> {
    wkt::types::LineString(
        line_string
            .coords()
            .map(|coord| coord_to_wkt(&coord))
            .collect(),
    )
}

fn polygon_to_wkt<T: CoordFloat>(polygon: &impl PolygonTrait<T = T>) -> wkt::types::Polygon<T> {
    let mut rings = vec![];
    if let Some(exterior) = polygon.exterior() {
        rings.push(line_string_to_wkt(&exterior));
    }
    polygon
        .interiors()
        .for_each(|interior| rings.push(line_string_to_wkt(&interior)));
    wkt::types::Polygon(rings)
}

fn multi_point_to_wkt<T: CoordFloat>(
    multi_point: &impl MultiPointTrait<T = T>,
) -> wkt::types::MultiPoint<T> {
    wkt::types::MultiPoint(
        multi_point
            .points()
            .map(|point| point_to_wkt(&point))
            .collect(),
    )
}

fn multi_line_string_to_wkt<T: CoordFloat>(
    multi_line_string: &impl MultiLineStringTrait<T = T>,
) -> wkt::types::MultiLineString<T> {
    wkt::types::MultiLineString(
        multi_line_string
            .lines()
            .map(|line| line_string_to_wkt(&line))
            .collect(),
    )
}

fn multi_polygon_to_wkt<T: CoordFloat>(
    multi_polygon: &impl MultiPolygonTrait<T = T>,
) -> wkt::types::MultiPolygon<T> {
    wkt::types::MultiPolygon(
        multi_polygon
            .polygons()
            .map(|polygon| polygon_to_wkt(&polygon))
            .collect(),
    )
}

/// Convert any Rect to a [`geo::Rect`].
///
/// Only the first two dimensions will be kept.
fn rect_to_wkt<T: CoordFloat>(_rect: &impl RectTrait<T = T>) -> wkt::types::Polygon<T> {
    todo!()
    // Need to create custom coords for a polygon box, see
    // https://github.com/georust/geo/blob/68f80f851879dd58f146aae47dc2feeea6c83230/geo-types/src/geometry/rect.rs#L217-L225
}

/// Convert any Geometry to a [`geo::Geometry`].
///
/// Only the first two dimensions will be kept.
fn geometry_to_wkt<T: CoordFloat>(geometry: &impl GeometryTrait<T = T>) -> wkt::Wkt<T> {
    match geometry.as_type() {
        GeometryType::Point(geom) => wkt::Wkt::Point(point_to_wkt(geom)),
        GeometryType::LineString(geom) => wkt::Wkt::LineString(line_string_to_wkt(geom)),
        GeometryType::Polygon(geom) => wkt::Wkt::Polygon(polygon_to_wkt(geom)),
        GeometryType::MultiPoint(geom) => wkt::Wkt::MultiPoint(multi_point_to_wkt(geom)),
        GeometryType::MultiLineString(geom) => {
            wkt::Wkt::MultiLineString(multi_line_string_to_wkt(geom))
        }
        GeometryType::MultiPolygon(geom) => wkt::Wkt::MultiPolygon(multi_polygon_to_wkt(geom)),
        GeometryType::GeometryCollection(geom) => {
            wkt::Wkt::GeometryCollection(geometry_collection_to_wkt(geom))
        }
        GeometryType::Rect(geom) => wkt::Wkt::Polygon(rect_to_wkt(geom)),
    }
}

/// Convert any GeometryCollection to a [`geo::GeometryCollection`].
///
/// Only the first two dimensions will be kept.
fn geometry_collection_to_wkt<T: CoordFloat>(
    geometry_collection: &impl GeometryCollectionTrait<T = T>,
) -> wkt::types::GeometryCollection<T> {
    wkt::types::GeometryCollection(
        geometry_collection
            .geometries()
            .map(|geometry| geometry_to_wkt(&geometry))
            .collect(),
    )
}
