use std::io::Write;

use arrow_json::Encoder;
use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::PointArray;

// An [Encoder] for [PointArray].
pub struct PointEncoder(PointArray);

impl PointEncoder {
    pub fn new(array: PointArray) -> Self {
        Self(array)
    }
}

impl Encoder for PointEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let point = self.0.value(idx).unwrap();
        encode_point(&point, out);
    }
}

pub(crate) fn encode_point(point: &impl PointTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type":"Point","coordinates":"#);
    let coord = point
        .coord()
        .expect("POINT EMPTY not yet supported in GeoJSON writer");
    encode_coord(&coord, out);
    out.push(b'}');
}

pub(crate) fn encode_coord(coord: &impl CoordTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    let dim_size = coord.dim().size();
    for n in 0..dim_size {
        write!(out, "{}", coord.nth_or_panic(n)).unwrap();
        if n < dim_size - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use geoarrow_array::test::point::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_point() {
        let mut encoder = PointEncoder::new(array(CoordType::Separated, Dimension::XY));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"Point","coordinates":[30,10]}"#;
        assert_eq!(s, expected);
    }

    #[test]
    fn encode_point_xyz() {
        let mut encoder = PointEncoder::new(array(CoordType::Separated, Dimension::XYZ));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"Point","coordinates":[30,10,40]}"#;
        assert_eq!(s, expected);
    }
}
