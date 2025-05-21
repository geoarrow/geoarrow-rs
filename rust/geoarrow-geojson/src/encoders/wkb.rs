use arrow_array::OffsetSizeTrait;
use arrow_json::Encoder;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::{GenericWkbArray, WkbViewArray};

use crate::encoders::geometry::encode_geometry;

pub(crate) struct GenericWkbEncoder<O: OffsetSizeTrait>(GenericWkbArray<O>);

impl<O: OffsetSizeTrait> GenericWkbEncoder<O> {
    pub(crate) fn new(array: GenericWkbArray<O>) -> Self {
        Self(array)
    }
}

impl<O: OffsetSizeTrait> Encoder for GenericWkbEncoder<O> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_geometry(&geom, out);
    }
}

pub(crate) struct WkbViewEncoder(WkbViewArray);

impl WkbViewEncoder {
    pub(crate) fn new(array: WkbViewArray) -> Self {
        Self(array)
    }
}

impl Encoder for WkbViewEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_geometry(&geom, out);
    }
}
