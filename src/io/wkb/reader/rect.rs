use crate::geo_traits::RectTrait;
use crate::io::wkb::reader::coord::WKBCoord;

/// This does not exist in the WKB specification, but is defined in order to conform WKBGeometry to
/// the GeometryTrait definition
pub struct WKBRect<'a> {
    _buf: &'a [u8],
}

impl<'a> RectTrait<'a> for WKBRect<'a> {
    type T = f64;
    type ItemType = WKBCoord<'a>;

    fn lower(&self) -> Self::ItemType {
        todo!()
    }

    fn upper(&self) -> Self::ItemType {
        todo!()
    }
}
