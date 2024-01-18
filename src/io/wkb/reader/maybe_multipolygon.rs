use crate::algorithm::native::eq::multi_polygon_eq;
use crate::geo_traits::MultiPolygonTrait;
use crate::io::wkb::reader::multipolygon::WKBMultiPolygon;
use crate::io::wkb::reader::polygon::WKBPolygon;

/// An WKB object that can be either a WKBPolygon or a WKBMultiPolygon.
///
/// This is used for casting a mix of polygons and multi polygons to an array of multi polygons
#[derive(Debug, Clone)]
pub enum WKBMaybeMultiPolygon<'a> {
    Polygon(WKBPolygon<'a>),
    MultiPolygon(WKBMultiPolygon<'a>),
}

impl<'a> WKBMaybeMultiPolygon<'a> {
    /// Check if this has equal coordinates as some other MultiPolygon object
    pub fn equals_multi_polygon(&self, other: &impl MultiPolygonTrait<T = f64>) -> bool {
        multi_polygon_eq(self, other)
    }
}

impl<'a> MultiPolygonTrait for WKBMaybeMultiPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBPolygon<'a> where Self: 'b;

    fn num_polygons(&self) -> usize {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.num_polygons(),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.num_polygons(),
        }
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.polygon_unchecked(i),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.polygon_unchecked(i),
        }
    }
}

impl<'a> MultiPolygonTrait for &'a WKBMaybeMultiPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBPolygon<'a> where Self: 'b;

    fn num_polygons(&self) -> usize {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.num_polygons(),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.num_polygons(),
        }
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        match self {
            WKBMaybeMultiPolygon::Polygon(geom) => geom.polygon_unchecked(i),
            WKBMaybeMultiPolygon::MultiPolygon(geom) => geom.polygon_unchecked(i),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::wkb::reader::geometry::Endianness;
    use crate::test::multipolygon::mp0;
    use crate::test::polygon::p0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn polygon_round_trip() {
        let geom = p0();
        let buf = geo::Geometry::Polygon(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom =
            WKBMaybeMultiPolygon::Polygon(WKBPolygon::new(&buf, Endianness::LittleEndian, 0));

        assert!(wkb_geom.equals_multi_polygon(&geo::MultiPolygon(vec![geom])));
    }

    #[test]
    fn multi_polygon_round_trip() {
        let geom = mp0();
        let buf = geo::Geometry::MultiPolygon(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBMaybeMultiPolygon::MultiPolygon(WKBMultiPolygon::new(
            &buf,
            Endianness::LittleEndian,
        ));

        assert!(wkb_geom.equals_multi_polygon(&geom));
    }
}
