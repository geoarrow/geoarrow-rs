use crate::geo_traits::RectTrait;
use crate::io::wkb::reader::coord::WKBCoord;

/// This does not exist in the WKB specification, but is defined in order to conform WKBGeometry to
/// the GeometryTrait definition
pub struct WKBRect<'a, B: AsRef<[u8]> + 'a> {
    _buf: B,
}

impl<'a, B: AsRef<[u8]> + 'a> RectTrait<'a> for WKBRect<'a, B> {
    type T = f64;
    type ItemType = WKBCoord<'a, B>;

    fn lower(&self) -> Self::ItemType {
        todo!()
    }

    fn upper(&self) -> Self::ItemType {
        todo!()
    }
}
