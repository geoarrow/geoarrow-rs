use arrow_json::Encoder;
use geo_traits::{LineStringTrait, MultiLineStringTrait};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::MultiLineStringArray;

use crate::encoders::linestring::encode_coords;

pub(crate) struct MultiLineStringEncoder(MultiLineStringArray);

impl MultiLineStringEncoder {
    pub(crate) fn new(array: MultiLineStringArray) -> Self {
        Self(array)
    }
}

impl Encoder for MultiLineStringEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_multi_line_string(&geom, out);
    }
}

/// Encode a MultiLineString geometry including the `type: MultiLineString` header
pub(crate) fn encode_multi_line_string(
    geom: &impl MultiLineStringTrait<T = f64>,
    out: &mut Vec<u8>,
) {
    out.extend(br#"{"type":"MultiLineString","coordinates":"#);
    encode_multi_line_string_inner(geom, out);
    out.push(b'}');
}

fn encode_multi_line_string_inner(geom: &impl MultiLineStringTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    let num_line_strings = geom.num_line_strings();
    for (idx, line_string) in geom.line_strings().enumerate() {
        encode_coords(line_string.coords(), out);
        if idx < num_line_strings - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use geoarrow_array::test::multilinestring::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_multi_line_string() {
        let mut encoder = MultiLineStringEncoder::new(array(CoordType::Separated, Dimension::XY));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"MultiLineString","coordinates":[[[30,10],[10,30],[40,40]]]}"#;
        assert_eq!(s, expected);
    }
}
