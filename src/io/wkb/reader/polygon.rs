use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::algorithm::native::eq::polygon_eq;
use crate::geo_traits::{MultiPolygonTrait, PolygonTrait};
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::reader::linearring::WKBLinearRing;

const WKB_POLYGON_TYPE: u32 = 3;

#[derive(Debug, Clone)]
pub struct WKBPolygon<'a> {
    wkb_linear_rings: Vec<WKBLinearRing<'a>>,
}

impl<'a> WKBPolygon<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(1 + offset);

        // Assert that this is indeed a 2D Polygon
        assert_eq!(
            WKB_POLYGON_TYPE,
            match byte_order {
                Endianness::BigEndian => reader.read_u32::<BigEndian>().unwrap(),
                Endianness::LittleEndian => reader.read_u32::<LittleEndian>().unwrap(),
            }
        );

        let num_rings = match byte_order {
            Endianness::BigEndian => reader.read_u32::<BigEndian>().unwrap().try_into().unwrap(),
            Endianness::LittleEndian => reader
                .read_u32::<LittleEndian>()
                .unwrap()
                .try_into()
                .unwrap(),
        };

        // - existing offset into buffer
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numLineStrings
        let mut ring_offset = offset + 1 + 4 + 4;
        let mut wkb_linear_rings = Vec::with_capacity(num_rings);
        for _ in 0..num_rings {
            let polygon = WKBLinearRing::new(buf, byte_order, ring_offset);
            wkb_linear_rings.push(polygon);
            ring_offset += polygon.size();
        }

        Self { wkb_linear_rings }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - size of each linear ring
        self.wkb_linear_rings
            .iter()
            .fold(1 + 4 + 4, |acc, ring| acc + ring.size())
    }

    pub fn is_empty(&self) -> bool {
        self.wkb_linear_rings.len() == 0
    }

    /// Check if this WKBPolygon has equal coordinates as some other Polygon object
    pub fn equals_polygon(&self, other: &impl PolygonTrait<T = f64>) -> bool {
        polygon_eq(self, other)
    }
}

impl<'a> PolygonTrait for WKBPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBLinearRing<'a>where Self: 'b;

    fn num_interiors(&self) -> usize {
        // Support an empty polygon with no rings
        if self.wkb_linear_rings.is_empty() {
            0
        } else {
            self.wkb_linear_rings.len() - 1
        }
    }

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        if self.wkb_linear_rings.is_empty() {
            None
        } else {
            Some(self.wkb_linear_rings[0])
        }
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        *self.wkb_linear_rings.get_unchecked(i + 1)
    }
}

impl<'a> PolygonTrait for &'a WKBPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBLinearRing<'a> where Self: 'b;

    fn num_interiors(&self) -> usize {
        // Support an empty polygon with no rings
        if self.wkb_linear_rings.is_empty() {
            0
        } else {
            self.wkb_linear_rings.len() - 1
        }
    }

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        if self.wkb_linear_rings.is_empty() {
            None
        } else {
            Some(self.wkb_linear_rings[0])
        }
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        *self.wkb_linear_rings.get_unchecked(i + 1)
    }
}

impl<'a> MultiPolygonTrait for WKBPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBPolygon<'a> where Self: 'b;

    fn num_polygons(&self) -> usize {
        1
    }

    unsafe fn polygon_unchecked(&self, _i: usize) -> Self::ItemType<'_> {
        self.clone()
    }
}

impl<'a> MultiPolygonTrait for &'a WKBPolygon<'a> {
    type T = f64;
    type ItemType<'b> = WKBPolygon<'a> where Self: 'b;

    fn num_polygons(&self) -> usize {
        1
    }

    unsafe fn polygon_unchecked(&self, _i: usize) -> Self::ItemType<'_> {
        // TODO: this looks bad
        #[allow(suspicious_double_ref_op)]
        self.clone().clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn polygon_round_trip() {
        let geom = p0();
        let buf = geo::Geometry::Polygon(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBPolygon::new(&buf, Endianness::LittleEndian, 0);

        assert!(wkb_geom.equals_polygon(&geom));
    }
}
