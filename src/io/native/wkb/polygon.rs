use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::PolygonTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::linearring::WKBLinearRing;

const WKB_POLYGON_TYPE: u32 = 3;

#[derive(Clone)]
pub struct WKBPolygon<'a> {
    wkb_linear_rings: Vec<WKBLinearRing<'a>>,
}

impl<'a> WKBPolygon<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(1);

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
}

impl<'a> PolygonTrait<'a> for WKBPolygon<'a> {
    type T = f64;
    type ItemType = WKBLinearRing<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_interiors(&self) -> usize {
        self.wkb_linear_rings.len() - 1
    }

    fn exterior(&self) -> Self::ItemType {
        self.wkb_linear_rings[0]
    }

    fn interior(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_interiors() {
            return None;
        }

        Some(self.wkb_linear_rings[i + 1])
    }

    fn interiors(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> PolygonTrait<'a> for &WKBPolygon<'a> {
    type T = f64;
    type ItemType = WKBLinearRing<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_interiors(&self) -> usize {
        self.wkb_linear_rings.len() - 1
    }

    fn exterior(&self) -> Self::ItemType {
        self.wkb_linear_rings[0]
    }

    fn interior(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_interiors() {
            return None;
        }

        Some(self.wkb_linear_rings[i + 1])
    }

    fn interiors(&'a self) -> Self::Iter {
        todo!()
    }
}
