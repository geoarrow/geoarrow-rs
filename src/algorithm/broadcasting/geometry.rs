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
use crate::scalar::Geometry;
use arrow_array::OffsetSizeTrait;

/// An enum over all broadcastable geometry types.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug)]
pub enum BroadcastableGeometry<'a, O: OffsetSizeTrait> {
    Point(BroadcastablePoint<'a>),
    LineString(BroadcastableLineString<'a, O>),
    Polygon(BroadcastablePolygon<'a, O>),
    MultiPoint(BroadcastableMultiPoint<'a, O>),
    MultiLineString(BroadcastableMultiLineString<'a, O>),
    MultiPolygon(BroadcastableMultiPolygon<'a, O>),
}

pub enum BroadcastGeometryIter<'a, O: OffsetSizeTrait> {
    Point(BroadcastPointIter<'a>),
    LineString(BroadcastLineStringIter<'a, O>),
    Polygon(BroadcastPolygonIter<'a, O>),
    MultiPoint(BroadcastMultiPointIter<'a, O>),
    MultiLineString(BroadcastMultiLineStringIter<'a, O>),
    MultiPolygon(BroadcastMultiPolygonIter<'a, O>),
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a BroadcastableGeometry<'a, O> {
    type Item = Option<Geometry<'a, O>>;
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

impl<'a, O: OffsetSizeTrait> Iterator for BroadcastGeometryIter<'a, O> {
    type Item = Option<Geometry<'a, O>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastGeometryIter::Point(p) => p.next().map(|g| g.map(Geometry::Point)),
            BroadcastGeometryIter::LineString(p) => p.next().map(|g| g.map(Geometry::LineString)),
            BroadcastGeometryIter::Polygon(p) => p.next().map(|g| g.map(Geometry::Polygon)),
            BroadcastGeometryIter::MultiPoint(p) => p.next().map(|g| g.map(Geometry::MultiPoint)),
            BroadcastGeometryIter::MultiLineString(p) => {
                p.next().map(|g| g.map(Geometry::MultiLineString))
            }
            BroadcastGeometryIter::MultiPolygon(p) => {
                p.next().map(|g| g.map(Geometry::MultiPolygon))
            }
        }
    }
}
