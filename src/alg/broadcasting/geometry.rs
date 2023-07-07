use crate::alg::broadcasting::linestring::BroadcastLineStringIter;
use crate::alg::broadcasting::multilinestring::BroadcastMultiLineStringIter;
use crate::alg::broadcasting::multipoint::BroadcastMultiPointIter;
use crate::alg::broadcasting::multipolygon::BroadcastMultiPolygonIter;
use crate::alg::broadcasting::point::BroadcastPointIter;
use crate::alg::broadcasting::polygon::BroadcastPolygonIter;
use crate::alg::broadcasting::{
    BroadcastableLineString, BroadcastableMultiLineString, BroadcastableMultiPoint,
    BroadcastableMultiPolygon, BroadcastablePoint, BroadcastablePolygon,
};

pub enum BroadcastableGeometry {
    Point(BroadcastablePoint),
    LineString(BroadcastableLineString),
    Polygon(BroadcastablePolygon),
    MultiPoint(BroadcastableMultiPoint),
    MultiLineString(BroadcastableMultiLineString),
    MultiPolygon(BroadcastableMultiPolygon),
}

pub enum BroadcastGeometryIter<'a> {
    Point(BroadcastPointIter<'a>),
    LineString(BroadcastLineStringIter<'a>),
    Polygon(BroadcastPolygonIter<'a>),
    MultiPoint(BroadcastMultiPointIter<'a>),
    MultiLineString(BroadcastMultiLineStringIter<'a>),
    MultiPolygon(BroadcastMultiPolygonIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastableGeometry {
    type Item = geo::Geometry;
    type IntoIter = BroadcastGeometryIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableGeometry::Point(p) => BroadcastGeometryIter::Point(p.into_iter()),
            BroadcastableGeometry::LineString(p) => {
                BroadcastGeometryIter::LineString(p.into_iter())
            }
            BroadcastableGeometry::Polygon(p) => BroadcastGeometryIter::Polygon(p.into_iter()),
            BroadcastableGeometry::MultiPoint(p) => {
                BroadcastGeometryIter::MultiPoint(p.into_iter())
            }
            BroadcastableGeometry::MultiLineString(p) => {
                BroadcastGeometryIter::MultiLineString(p.into_iter())
            }
            BroadcastableGeometry::MultiPolygon(p) => {
                BroadcastGeometryIter::MultiPolygon(p.into_iter())
            }
        }
    }
}

impl<'a> Iterator for BroadcastGeometryIter<'a> {
    type Item = geo::Geometry;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastGeometryIter::Point(p) => p.next().map(geo::Geometry::Point),
            BroadcastGeometryIter::LineString(p) => p.next().map(geo::Geometry::LineString),
            BroadcastGeometryIter::Polygon(p) => p.next().map(geo::Geometry::Polygon),
            BroadcastGeometryIter::MultiPoint(p) => p.next().map(geo::Geometry::MultiPoint),
            BroadcastGeometryIter::MultiLineString(p) => {
                p.next().map(geo::Geometry::MultiLineString)
            }
            BroadcastGeometryIter::MultiPolygon(p) => p.next().map(geo::Geometry::MultiPolygon),
        }
    }
}
