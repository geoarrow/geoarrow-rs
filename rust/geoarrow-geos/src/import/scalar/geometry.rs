use geo_traits::{GeometryTrait, UnimplementedLine, UnimplementedRect, UnimplementedTriangle};
use geos::Geom;

use crate::import::scalar::geometrycollection::GEOSGeometryCollection;
use crate::import::scalar::linestring::GEOSLineString;
use crate::import::scalar::multilinestring::GEOSMultiLineString;
use crate::import::scalar::multipoint::GEOSMultiPoint;
use crate::import::scalar::multipolygon::GEOSMultiPolygon;
use crate::import::scalar::point::GEOSPoint;
use crate::import::scalar::polygon::GEOSPolygon;

pub enum GEOSGeometry {
    Point(GEOSPoint),
    LineString(GEOSLineString),
    Polygon(GEOSPolygon),
    MultiPoint(GEOSMultiPoint),
    MultiLineString(GEOSMultiLineString),
    MultiPolygon(GEOSMultiPolygon),
    GeometryCollection(GEOSGeometryCollection),
}

impl GEOSGeometry {
    pub fn new(geom: geos::Geometry) -> Self {
        match geom.geometry_type() {
            geos::GeometryTypes::Point => Self::Point(GEOSPoint::new_unchecked(geom)),
            geos::GeometryTypes::LineString => {
                Self::LineString(GEOSLineString::new_unchecked(geom))
            }
            geos::GeometryTypes::Polygon => Self::Polygon(GEOSPolygon::new_unchecked(geom)),
            geos::GeometryTypes::MultiPoint => {
                Self::MultiPoint(GEOSMultiPoint::new_unchecked(geom))
            }
            geos::GeometryTypes::MultiLineString => {
                Self::MultiLineString(GEOSMultiLineString::new_unchecked(geom))
            }
            geos::GeometryTypes::MultiPolygon => {
                Self::MultiPolygon(GEOSMultiPolygon::new_unchecked(geom))
            }
            geos::GeometryTypes::GeometryCollection => {
                Self::GeometryCollection(GEOSGeometryCollection::new_unchecked(geom))
            }
            geos::GeometryTypes::LinearRing => panic!("GEOS Linear ring not supported"),
            geos::GeometryTypes::__Unknown(x) => panic!("Unknown geometry type {x}"),
        }
    }
}

impl GeometryTrait for GEOSGeometry {
    type T = f64;
    type PointType<'a> = GEOSPoint;
    type LineStringType<'a> = GEOSLineString;
    type PolygonType<'a> = GEOSPolygon;
    type MultiPointType<'a> = GEOSMultiPoint;
    type MultiLineStringType<'a> = GEOSMultiLineString;
    type MultiPolygonType<'a> = GEOSMultiPolygon;
    type GeometryCollectionType<'a> = GEOSGeometryCollection;
    type RectType<'a> = UnimplementedRect<f64>;
    type LineType<'a> = UnimplementedLine<f64>;
    type TriangleType<'a> = UnimplementedTriangle<f64>;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Self::Point(g) => g.dim(),
            Self::LineString(g) => g.dim(),
            Self::Polygon(g) => g.dim(),
            Self::MultiPoint(g) => g.dim(),
            Self::MultiLineString(g) => g.dim(),
            Self::MultiPolygon(g) => g.dim(),
            Self::GeometryCollection(g) => g.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        GEOSPoint,
        GEOSLineString,
        GEOSPolygon,
        GEOSMultiPoint,
        GEOSMultiLineString,
        GEOSMultiPolygon,
        GEOSGeometryCollection,
        UnimplementedRect<f64>,
        UnimplementedTriangle<f64>,
        UnimplementedLine<f64>,
    > {
        match self {
            Self::Point(g) => geo_traits::GeometryType::Point(g),
            Self::LineString(g) => geo_traits::GeometryType::LineString(g),
            Self::Polygon(g) => geo_traits::GeometryType::Polygon(g),
            Self::MultiPoint(g) => geo_traits::GeometryType::MultiPoint(g),
            Self::MultiLineString(g) => geo_traits::GeometryType::MultiLineString(g),
            Self::MultiPolygon(g) => geo_traits::GeometryType::MultiPolygon(g),
            Self::GeometryCollection(g) => geo_traits::GeometryType::GeometryCollection(g),
        }
    }
}

// Specialized implementations on each WKT concrete type.

macro_rules! impl_specialization {
    ($geometry_type:ident, $geometry_variant:ident) => {
        impl<'a> GeometryTrait for $geometry_type {
            type T = f64;
            type PointType<'b>
                = GEOSPoint
            where
                Self: 'b;
            type LineStringType<'b>
                = GEOSLineString
            where
                Self: 'b;
            type PolygonType<'b>
                = GEOSPolygon
            where
                Self: 'b;
            type MultiPointType<'b>
                = GEOSMultiPoint
            where
                Self: 'b;
            type MultiLineStringType<'b>
                = GEOSMultiLineString
            where
                Self: 'b;
            type MultiPolygonType<'b>
                = GEOSMultiPolygon
            where
                Self: 'b;
            type GeometryCollectionType<'b>
                = GEOSGeometryCollection
            where
                Self: 'b;
            type RectType<'b>
                = geo_traits::UnimplementedRect<f64>
            where
                Self: 'b;
            type LineType<'b>
                = geo_traits::UnimplementedLine<f64>
            where
                Self: 'b;
            type TriangleType<'b>
                = geo_traits::UnimplementedTriangle<f64>
            where
                Self: 'b;

            fn dim(&self) -> geo_traits::Dimensions {
                self.dimension().into()
            }

            fn as_type(
                &self,
            ) -> geo_traits::GeometryType<
                '_,
                Self::PointType<'_>,
                Self::LineStringType<'_>,
                Self::PolygonType<'_>,
                Self::MultiPointType<'_>,
                Self::MultiLineStringType<'_>,
                Self::MultiPolygonType<'_>,
                Self::GeometryCollectionType<'_>,
                Self::RectType<'_>,
                Self::TriangleType<'_>,
                Self::LineType<'_>,
            > {
                geo_traits::GeometryType::$geometry_variant(self)
            }
        }

        impl<'a> GeometryTrait for &$geometry_type {
            type T = f64;
            type PointType<'b>
                = GEOSPoint
            where
                Self: 'b;
            type LineStringType<'b>
                = GEOSLineString
            where
                Self: 'b;
            type PolygonType<'b>
                = GEOSPolygon
            where
                Self: 'b;
            type MultiPointType<'b>
                = GEOSMultiPoint
            where
                Self: 'b;
            type MultiLineStringType<'b>
                = GEOSMultiLineString
            where
                Self: 'b;
            type MultiPolygonType<'b>
                = GEOSMultiPolygon
            where
                Self: 'b;
            type GeometryCollectionType<'b>
                = GEOSGeometryCollection
            where
                Self: 'b;
            type RectType<'b>
                = geo_traits::UnimplementedRect<f64>
            where
                Self: 'b;
            type LineType<'b>
                = geo_traits::UnimplementedLine<f64>
            where
                Self: 'b;
            type TriangleType<'b>
                = geo_traits::UnimplementedTriangle<f64>
            where
                Self: 'b;

            fn dim(&self) -> geo_traits::Dimensions {
                self.dimension().into()
            }

            fn as_type(
                &self,
            ) -> geo_traits::GeometryType<
                '_,
                Self::PointType<'_>,
                Self::LineStringType<'_>,
                Self::PolygonType<'_>,
                Self::MultiPointType<'_>,
                Self::MultiLineStringType<'_>,
                Self::MultiPolygonType<'_>,
                Self::GeometryCollectionType<'_>,
                Self::RectType<'_>,
                Self::TriangleType<'_>,
                Self::LineType<'_>,
            > {
                geo_traits::GeometryType::$geometry_variant(self)
            }
        }
    };
}

impl_specialization!(GEOSPoint, Point);
impl_specialization!(GEOSLineString, LineString);
impl_specialization!(GEOSPolygon, Polygon);
impl_specialization!(GEOSMultiPoint, MultiPoint);
impl_specialization!(GEOSMultiLineString, MultiLineString);
impl_specialization!(GEOSMultiPolygon, MultiPolygon);
impl_specialization!(GEOSGeometryCollection, GeometryCollection);
