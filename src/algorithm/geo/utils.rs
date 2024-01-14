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

pub fn coord_to_geo<T: CoordNum>(coord: &impl CoordTrait<T = T>) -> geo::Coord<T> {
    geo::Coord {
        x: coord.x(),
        y: coord.y(),
    }
}

pub fn point_to_geo<T: CoordNum>(point: &impl PointTrait<T = T>) -> geo::Point<T> {
    geo::Point::new(point.x(), point.y())
}

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

pub fn polygon_to_geo<T: CoordNum>(polygon: &impl PolygonTrait<T = T>) -> geo::Polygon<T> {
    let exterior = line_string_to_geo(&polygon.exterior().unwrap());
    let interiors = polygon
        .interiors()
        .map(|interior| line_string_to_geo(&interior))
        .collect();
    geo::Polygon::new(exterior, interiors)
}

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

pub fn multi_line_string_to_geo<T: CoordNum>(
    multi_line_string: &impl MultiLineStringTrait<T = T>,
) -> geo::MultiLineString<T> {
    geo::MultiLineString::new(
        multi_line_string
            .lines()
            .map(|line| line_string_to_geo(&line))
            .collect(),
    )
}

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
    geo::GeometryCollection::new_from(
        geometry_collection
            .geometries()
            .map(|geometry| geometry_to_geo(&geometry))
            .collect(),
    )
}
