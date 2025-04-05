use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use geo_traits::GeometryTrait;

use crate::error::Result;

/// A scalar WKB reference on a WKBArray
///
/// This is zero-cost to _create_ from a [WKBArray] but the WKB has not been preprocessed yet, so
/// it's not constant-time to access coordinate values.
///
/// This does not directly implement [GeometryTrait], because it first needs to be parsed. Use
/// [`parse`] to access an opaque object that does implement [GeometryTrait].
///
/// [`parse`]: WKB::parse
#[derive(Debug, Clone)]
pub struct WKB<'a, O: OffsetSizeTrait> {
    pub(crate) arr: &'a GenericBinaryArray<O>,
    pub(crate) geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> WKB<'a, O> {
    /// Construct a new WKB.
    pub(crate) fn new(arr: &'a GenericBinaryArray<O>, geom_index: usize) -> Self {
        Self { arr, geom_index }
    }

    /// Access the byte slice of this WKB object.
    pub fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    pub(crate) fn into_owned_inner(self) -> (GenericBinaryArray<O>, usize) {
        // TODO: hard slice?
        // let owned = self.into_owned();
        (self.arr.clone(), self.geom_index)
    }

    /// Parse this WKB buffer to a geometry.
    pub fn parse(&self) -> Result<impl GeometryTrait<T = f64> + use<'_, O>> {
        Ok(wkb::reader::read_wkb(self.as_ref())?)
    }
}

impl<O: OffsetSizeTrait> AsRef<[u8]> for WKB<'_, O> {
    fn as_ref(&self) -> &[u8] {
        self.arr.value(self.geom_index)
    }
}

impl<O: OffsetSizeTrait> PartialEq for WKB<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        self.arr.value(self.geom_index) == other.arr.value(other.geom_index)
    }
}
