use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::datatypes::Dimension;
use crate::error::Result;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::reader::geometry::{Endianness, WKBGeometry};

/// skip endianness and wkb type
const HEADER_BYTES: u64 = 5;

/// A WKB GeometryCollection
#[derive(Debug, Clone)]
pub struct WKBGeometryCollection<'a> {
    /// A WKBGeometry object for each of the internal geometries
    geometries: Vec<WKBGeometry<'a>>,
    dim: Dimension,
}

impl<'a> WKBGeometryCollection<'a> {
    pub fn try_new(buf: &'a [u8], byte_order: Endianness, dim: Dimension) -> Result<Self> {
        let mut reader = Cursor::new(buf);
        reader.set_position(HEADER_BYTES);
        let num_geometries = match byte_order {
            Endianness::BigEndian => reader.read_u32::<BigEndian>().unwrap().try_into().unwrap(),
            Endianness::LittleEndian => reader
                .read_u32::<LittleEndian>()
                .unwrap()
                .try_into()
                .unwrap(),
        };

        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numGeometries
        let mut geometry_offset = 1 + 4 + 4;
        let mut geometries = Vec::with_capacity(num_geometries);
        for _ in 0..num_geometries {
            let geometry = WKBGeometry::try_new(&buf[geometry_offset..])?;
            geometry_offset += geometry.size() as usize;
            geometries.push(geometry);
        }

        Ok(Self { geometries, dim })
    }

    pub fn dimension(&self) -> Dimension {
        self.dim
    }

    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numGeometries
        self.geometries
            .iter()
            .fold(1 + 4 + 4, |acc, x| acc + x.size())
    }
}

impl<'a> GeometryCollectionTrait for WKBGeometryCollection<'a> {
    type T = f64;
    type GeometryType<'b> = &'b WKBGeometry<'b> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_geometries(&self) -> usize {
        self.geometries.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        &self.geometries[i]
    }
}
