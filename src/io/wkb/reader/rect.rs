use crate::geo_traits::RectTrait;
use crate::io::wkb::reader::coord::WKBCoord;

/// This does not exist in the WKB specification, but is defined in order to conform WKBGeometry to
/// the GeometryTrait definition
pub struct WKBRect<'a> {
    _buf: &'a [u8],
}

impl<'a> RectTrait for WKBRect<'a> {
    type T = f64;
    type ItemType<'b> = WKBCoord<'a> where Self: 'b;

    fn lower(&self) -> Self::ItemType<'_> {
        todo!()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        todo!()
    }
}
