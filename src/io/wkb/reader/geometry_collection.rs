use crate::datatypes::Dimension;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::reader::geometry::{Endianness, WKBGeometry};

/// Not yet implemented but required for WKBGeometry
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct WKBGeometryCollection<'a> {
    buf: &'a [u8],
    byte_order: Endianness,
    dim: Dimension,
}

impl<'a> WKBGeometryCollection<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, dim: Dimension) -> Self {
        Self {
            buf,
            byte_order,
            dim,
        }
    }
}

impl<'a> GeometryCollectionTrait for WKBGeometryCollection<'a> {
    type T = f64;
    type ItemType<'b> = WKBGeometry<'a> where Self: 'b;

    fn dim(&self) -> usize {
        self.dim.size()
    }

    fn num_geometries(&self) -> usize {
        todo!()
    }

    unsafe fn geometry_unchecked(&self, _i: usize) -> Self::ItemType<'_> {
        todo!()
    }
}
