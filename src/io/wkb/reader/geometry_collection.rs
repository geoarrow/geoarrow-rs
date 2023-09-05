use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::reader::geometry::{Endianness, WKBGeometry};
use std::iter::Cloned;
use std::slice::Iter;

/// Not yet implemented but required for WKBGeometry
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct WKBGeometryCollection<'a, B: AsRef<[u8]> + 'a> {
    buf: B,
    byte_order: Endianness,
}

impl<'a, B: AsRef<[u8]> + 'a> WKBGeometryCollection<'a, B> {
    pub fn new(buf: B, byte_order: Endianness) -> Self {
        Self { buf, byte_order }
    }
}

impl<'a, B: AsRef<[u8]> + 'a> GeometryCollectionTrait<'a> for WKBGeometryCollection<'a, B> {
    type T = f64;
    type ItemType = WKBGeometry<'a, B>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_geometries(&self) -> usize {
        todo!()
    }

    fn geometry(&self, _i: usize) -> Option<Self::ItemType> {
        todo!()
    }

    fn geometries(&'a self) -> Self::Iter {
        todo!()
    }
}
