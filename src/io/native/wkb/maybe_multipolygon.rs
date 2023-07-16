use crate::geo_traits::MultiPolygonTrait;
use crate::io::native::wkb::multipolygon::WKBMultiPolygon;
use crate::io::native::wkb::polygon::WKBPolygon;
use std::iter::Cloned;
use std::slice::Iter;

/// An WKB object that can be either a WKBPolygon or a WKBMultiPolygon.
///
/// This is used for casting a mix of polygons and multi polygons to an array of multi polygons
#[derive(Debug, Clone)]
pub enum WKBMaybeMultiPolygon<'a> {
    Polygon(WKBPolygon<'a>),
    MultiPolygon(WKBMultiPolygon<'a>),
}

impl<'a> MultiPolygonTrait<'a> for WKBMaybeMultiPolygon<'a> {
    type T = f64;
    type ItemType = WKBPolygon<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_polygons(&self) -> usize {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.num_polygons(),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.num_polygons(),
        }
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType> {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.polygon(i),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.polygon(i),
        }
    }

    fn polygons(&'a self) -> Self::Iter {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.polygons(),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.polygons(),
        }
    }
}

impl<'a> MultiPolygonTrait<'a> for &WKBMaybeMultiPolygon<'a> {
    type T = f64;
    type ItemType = WKBPolygon<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_polygons(&self) -> usize {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.num_polygons(),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.num_polygons(),
        }
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType> {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.polygon(i),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.polygon(i),
        }
    }

    fn polygons(&'a self) -> Self::Iter {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.polygons(),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.polygons(),
        }
    }
}
