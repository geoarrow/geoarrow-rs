use arrow_json::Encoder;
use geo_traits::LineStringTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::LineStringArray;

use crate::encoders::point::encode_coord;

pub(crate) struct LineStringEncoder(LineStringArray);

impl LineStringEncoder {
    pub(crate) fn new(array: LineStringArray) -> Self {
        Self(array)
    }
}

impl Encoder for LineStringEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_line_string(&geom, out);
    }
}

/// Encode a LineString geometry including the `type: LineString` header
fn encode_line_string(geom: &impl LineStringTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type":"LineString","coordinates":"#);
    encode_line_string_coords(geom, out);
    out.push(b'}');
}

/// Encode the coordinates of a LineString geometry
pub(crate) fn encode_line_string_coords(geom: &impl LineStringTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    let num_coords = geom.num_coords();
    for (idx, coord) in geom.coords().enumerate() {
        encode_coord(&coord, out);
        if idx < num_coords - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use geoarrow_array::test::linestring::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_linestring() {
        let mut encoder = LineStringEncoder::new(array(CoordType::Separated, Dimension::XY));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"LineString","coordinates":[[30,10],[10,30],[40,40]]}"#;
        assert_eq!(s, expected);
    }
}
