use arrow_array::OffsetSizeTrait;
use arrow_json::Encoder;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::{GenericWktArray, WktViewArray};

use crate::encoders::geometry::encode_geometry;

pub(crate) struct GenericWktEncoder<O: OffsetSizeTrait>(GenericWktArray<O>);

impl<O: OffsetSizeTrait> GenericWktEncoder<O> {
    pub(crate) fn new(array: GenericWktArray<O>) -> Self {
        Self(array)
    }
}

impl<O: OffsetSizeTrait> Encoder for GenericWktEncoder<O> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_geometry(&geom, out);
    }
}

pub(crate) struct WktViewEncoder(WktViewArray);

impl WktViewEncoder {
    pub(crate) fn new(array: WktViewArray) -> Self {
        Self(array)
    }
}

impl Encoder for WktViewEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_geometry(&geom, out);
    }
}

#[cfg(test)]
mod test {
    use geoarrow_array::cast::to_wkt;
    use geoarrow_array::test::geometry::array;
    use geoarrow_schema::CoordType;

    use super::*;

    #[test]
    fn encode_geometry() {
        let geom_array = array(CoordType::Separated, false);
        let wkt_arr = to_wkt::<i32>(&geom_array).unwrap();
        let mut encoder = GenericWktEncoder::new(wkt_arr);

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"Point","coordinates":[30,10]}"#;
        assert_eq!(s, expected);
    }
}
