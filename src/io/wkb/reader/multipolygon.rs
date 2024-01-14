use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::algorithm::native::eq::multi_polygon_eq;
use crate::geo_traits::MultiPolygonTrait;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::reader::polygon::WKBPolygon;

const HEADER_BYTES: u64 = 5;

#[derive(Debug, Clone)]
pub struct WKBMultiPolygon<'a> {
    // buf: &'a [u8],
    // byte_order: Endianness,

    // /// The number of polygons in this MultiPolygon
    // num_polygons: usize,

    // /// The offset in the buffer where each WKBPolygon object begins
    // ///
    // /// The length of this vec must match the number of polygons
    // // polygon_offsets: Vec<usize>,
    /// A WKBPolygon object for each of the internal line strings
    wkb_polygons: Vec<WKBPolygon<'a>>,
}

impl<'a> WKBMultiPolygon<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(HEADER_BYTES);
        let num_polygons = match byte_order {
            Endianness::BigEndian => reader.read_u32::<BigEndian>().unwrap().try_into().unwrap(),
            Endianness::LittleEndian => reader
                .read_u32::<LittleEndian>()
                .unwrap()
                .try_into()
                .unwrap(),
        };

        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numLineStrings
        let mut polygon_offset = 1 + 4 + 4;
        let mut wkb_polygons = Vec::with_capacity(num_polygons);
        for _ in 0..num_polygons {
            let polygon = WKBPolygon::new(buf, byte_order, polygon_offset);
            polygon_offset += polygon.size();
            wkb_polygons.push(polygon);
        }

        Self { wkb_polygons }
    }

    /// Check if this WKBMultiLineString has equal coordinates as some other MultiLineString object
    pub fn equals_multi_polygon(&self, other: &impl MultiPolygonTrait<T = f64>) -> bool {
        multi_polygon_eq(self, other)
    }
}

impl<'a> MultiPolygonTrait for WKBMultiPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBPolygon<'a> where Self: 'b;

    fn num_polygons(&self) -> usize {
        self.wkb_polygons.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.wkb_polygons.get_unchecked(i).clone()
    }
}

impl<'a> MultiPolygonTrait for &'a WKBMultiPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBPolygon<'a> where Self: 'b;

    fn num_polygons(&self) -> usize {
        self.wkb_polygons.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.wkb_polygons.get_unchecked(i).clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipolygon::mp0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn multi_polygon_round_trip() {
        let geom = mp0();
        let buf = geo::Geometry::MultiPolygon(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBMultiPolygon::new(&buf, Endianness::LittleEndian);

        assert!(wkb_geom.equals_multi_polygon(&geom));
    }
}
