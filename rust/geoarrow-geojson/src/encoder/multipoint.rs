use arrow_json::Encoder;
use geo_traits::{MultiPointTrait, PointTrait};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::MultiPointArray;

use crate::encoder::point::encode_coord;

// An [Encoder] for [MultiPointArray].
pub struct MultiPointEncoder(MultiPointArray);

impl MultiPointEncoder {
    pub fn new(array: MultiPointArray) -> Self {
        Self(array)
    }
}

impl Encoder for MultiPointEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_multi_point(&geom, out);
    }
}

/// Encode a MultiPoint geometry including the `type: MultiPoint` header
pub(crate) fn encode_multi_point(geom: &impl MultiPointTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type":"MultiPoint","coordinates":"#);
    encode_points(geom.points(), out);
    out.push(b'}');
}

/// Encode the coordinates of a LineString geometry
fn encode_points(
    points: impl ExactSizeIterator<Item = impl PointTrait<T = f64>>,
    out: &mut Vec<u8>,
) {
    out.push(b'[');
    let num_coords = points.len();
    for (idx, point) in points.enumerate() {
        encode_coord(&point.coord().expect("Empty points not supported."), out);
        if idx < num_coords - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use geoarrow_array::test::multipoint::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_multipoint() {
        let mut encoder = MultiPointEncoder::new(array(CoordType::Separated, Dimension::XY));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"MultiPoint","coordinates":[[30,10]]}"#;
        assert_eq!(s, expected);

        geojson::Geometry::from_str(expected).expect("Should be valid GeoJSON");
    }
}
