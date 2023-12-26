use arrow_array::Float64Array;
use arrow_buffer::NullBuffer;
use geo::CoordNum;

use crate::geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

pub(crate) fn zeroes(len: usize, nulls: Option<&NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls.cloned())
}

/// Implements the common pattern where a [`GeometryArray`][crate::array::GeometryArray] enum
/// simply delegates its trait impl to it's inner type.
///
// This is derived from geo https://github.com/georust/geo/blob/d4c858308ba910f69beab175e08af263b17c5f9f/geo/src/types.rs#L119-L158
#[macro_export]
macro_rules! geometry_array_delegate_impl {
    ($($a:tt)*) => { $crate::__geometry_array_delegate_impl_helper!{ GeometryArray, $($a)* } }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __geometry_array_delegate_impl_helper {
    (
        $enum:ident,
        $(
            $(#[$outer:meta])*
            fn $func_name: ident(&$($self_life:lifetime)?self $(, $arg_name: ident: $arg_type: ty)*) -> $return: ty;
         )+
    ) => {
            $(
                $(#[$outer])*
                fn $func_name(&$($self_life)? self, $($arg_name: $arg_type),*) -> $return {
                    match self {
                        $enum::Point(g) => g.$func_name($($arg_name),*).into(),
                        // $enum::Line(g) =>  g.$func_name($($arg_name),*).into(),
                        $enum::LineString(g) => g.$func_name($($arg_name),*).into(),
                        $enum::Polygon(g) => g.$func_name($($arg_name),*).into(),
                        $enum::MultiPoint(g) => g.$func_name($($arg_name),*).into(),
                        $enum::MultiLineString(g) => g.$func_name($($arg_name),*).into(),
                        $enum::MultiPolygon(g) => g.$func_name($($arg_name),*).into(),
                        // $enum::GeometryCollection(g) => g.$func_name($($arg_name),*).into(),
                        $enum::Rect(_g) => todo!(),
                        // $enum::Rect(g) => g.$func_name($($arg_name),*).into(),
                        // $enum::Triangle(g) => g.$func_name($($arg_name),*).into(),
                    }
                }
            )+
        };
}

pub fn point_to_geo<T: CoordNum>(point: &impl PointTrait<T = T>) -> geo::Point<T> {
    geo::Point::new(point.x(), point.y())
}

pub fn line_string_to_geo<T: CoordNum>(
    line_string: &impl LineStringTrait<T = T>,
) -> geo::LineString<T> {
    let mut coords = Vec::with_capacity(line_string.num_coords());
    for coord_idx in 0..line_string.num_coords() {
        let c = line_string.coord(coord_idx).unwrap();
        coords.push(geo::Coord { x: c.x(), y: c.y() })
    }
    geo::LineString::new(coords)
}

pub fn polygon_to_geo<T: CoordNum>(polygon: &impl PolygonTrait<T = T>) -> geo::Polygon<T> {
    let exterior = line_string_to_geo(&polygon.exterior().unwrap());
    let mut interiors = Vec::with_capacity(polygon.num_interiors());
    for interior_idx in 0..polygon.num_interiors() {
        let int = polygon.interior(interior_idx).unwrap();
        interiors.push(line_string_to_geo(&int));
    }
    geo::Polygon::new(exterior, interiors)
}

pub fn multi_point_to_geo<T: CoordNum>(
    multi_point: &impl MultiPointTrait<T = T>,
) -> geo::MultiPoint<T> {
    let mut points = Vec::with_capacity(multi_point.num_points());
    for point_idx in 0..multi_point.num_points() {
        points.push(point_to_geo(&multi_point.point(point_idx).unwrap()));
    }
    geo::MultiPoint::new(points)
}

pub fn multi_line_string_to_geo<T: CoordNum>(
    multi_line_string: &impl MultiLineStringTrait<T = T>,
) -> geo::MultiLineString<T> {
    let mut line_strings = Vec::with_capacity(multi_line_string.num_lines());
    for line_string_idx in 0..multi_line_string.num_lines() {
        line_strings.push(line_string_to_geo(
            &multi_line_string.line(line_string_idx).unwrap(),
        ));
    }
    geo::MultiLineString::new(line_strings)
}

pub fn multi_polygon_to_geo<T: CoordNum>(
    multi_polygon: &impl MultiPolygonTrait<T = T>,
) -> geo::MultiPolygon<T> {
    let mut polygons = Vec::with_capacity(multi_polygon.num_polygons());
    for polygon_idx in 0..multi_polygon.num_polygons() {
        polygons.push(polygon_to_geo(&multi_polygon.polygon(polygon_idx).unwrap()));
    }
    geo::MultiPolygon::new(polygons)
}

pub fn rect_to_geo<T: CoordNum>(rect: &impl RectTrait<T = T>) -> geo::Rect<T> {
    let lower = rect.lower();
    let upper = rect.upper();

    let c1 = geo::Coord {
        x: lower.x(),
        y: lower.y(),
    };
    let c2 = geo::Coord {
        x: upper.x(),
        y: upper.y(),
    };

    geo::Rect::new(c1, c2)
}

pub fn geometry_to_geo<T: CoordNum>(geometry: &impl GeometryTrait<T = T>) -> geo::Geometry<T> {
    match geometry.as_type() {
        GeometryType::Point(geom) => geo::Geometry::Point(point_to_geo(geom)),
        GeometryType::LineString(geom) => geo::Geometry::LineString(line_string_to_geo(geom)),
        GeometryType::Polygon(geom) => geo::Geometry::Polygon(polygon_to_geo(geom)),
        GeometryType::MultiPoint(geom) => geo::Geometry::MultiPoint(multi_point_to_geo(geom)),
        GeometryType::MultiLineString(geom) => {
            geo::Geometry::MultiLineString(multi_line_string_to_geo(geom))
        }
        GeometryType::MultiPolygon(geom) => geo::Geometry::MultiPolygon(multi_polygon_to_geo(geom)),
        GeometryType::GeometryCollection(geom) => {
            geo::Geometry::GeometryCollection(geometry_collection_to_geo(geom))
        }
        GeometryType::Rect(geom) => geo::Geometry::Rect(rect_to_geo(geom)),
    }
}

pub fn geometry_collection_to_geo<T: CoordNum>(
    geometry_collection: &impl GeometryCollectionTrait<T = T>,
) -> geo::GeometryCollection<T> {
    let mut geometries = Vec::with_capacity(geometry_collection.num_geometries());
    for geometry_idx in 0..geometry_collection.num_geometries() {
        geometries.push(geometry_to_geo(
            &geometry_collection.geometry(geometry_idx).unwrap(),
        ));
    }
    geo::GeometryCollection::new_from(geometries)
}
