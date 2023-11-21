use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::reader::geometry::{Endianness, WKBGeometry};
use std::iter::Cloned;
use std::slice::Iter;

/// Not yet implemented but required for WKBGeometry
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct WKBGeometryCollection<'a> {
    buf: &'a [u8],
    byte_order: Endianness,
}

impl<'a> WKBGeometryCollection<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness) -> Self {
        Self { buf, byte_order }
    }
}

impl<'a> GeometryCollectionTrait for WKBGeometryCollection<'a> {
    type T = f64;
    type ItemType<'b> = WKBGeometry<'a> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_geometries(&self) -> usize {
        todo!()
    }

    fn geometry(&self, _i: usize) -> Option<Self::ItemType<'_>> {
        todo!()
    }

    fn geometries(&self) -> Self::Iter<'_> {
        todo!()
    }
}
