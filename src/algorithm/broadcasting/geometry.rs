use arrow2::types::Offset;

use crate::algorithm::broadcasting::linestring::BroadcastLineStringIter;
use crate::algorithm::broadcasting::multilinestring::BroadcastMultiLineStringIter;
use crate::algorithm::broadcasting::multipoint::BroadcastMultiPointIter;
use crate::algorithm::broadcasting::multipolygon::BroadcastMultiPolygonIter;
use crate::algorithm::broadcasting::point::BroadcastPointIter;
use crate::algorithm::broadcasting::polygon::BroadcastPolygonIter;
use crate::algorithm::broadcasting::{
    BroadcastableLineString, BroadcastableMultiLineString, BroadcastableMultiPoint,
    BroadcastableMultiPolygon, BroadcastablePoint, BroadcastablePolygon,
};

pub enum BroadcastableGeometry<O: Offset> {
    Point(BroadcastablePoint),
    LineString(BroadcastableLineString<O>),
    Polygon(BroadcastablePolygon<O>),
    MultiPoint(BroadcastableMultiPoint<O>),
    MultiLineString(BroadcastableMultiLineString<O>),
    MultiPolygon(BroadcastableMultiPolygon<O>),
}

pub enum BroadcastGeometryIter<'a, O: Offset> {
    Point(BroadcastPointIter<'a>),
    LineString(BroadcastLineStringIter<'a, O>),
    Polygon(BroadcastPolygonIter<'a, O>),
    MultiPoint(BroadcastMultiPointIter<'a, O>),
    MultiLineString(BroadcastMultiLineStringIter<'a, O>),
    MultiPolygon(BroadcastMultiPolygonIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableGeometry<O> {
    type Item = geo::Geometry;
    type IntoIter = BroadcastGeometryIter<'a, O>;

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

impl<'a, O: Offset> Iterator for BroadcastGeometryIter<'a, O> {
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
