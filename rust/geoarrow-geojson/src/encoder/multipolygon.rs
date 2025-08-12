use arrow_json::Encoder;
use geo_traits::MultiPolygonTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::MultiPolygonArray;

use crate::encoder::polygon::encode_polygon_rings;

// An [Encoder] for [MultiPolygonArray].
pub struct MultiPolygonEncoder(MultiPolygonArray);

impl MultiPolygonEncoder {
    pub fn new(array: MultiPolygonArray) -> Self {
        Self(array)
    }
}

impl Encoder for MultiPolygonEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_multi_polygon(&geom, out);
    }
}

/// Encode a MultiPolygon geometry including the `type: MultiPolygon` header
pub(crate) fn encode_multi_polygon(geom: &impl MultiPolygonTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type":"MultiPolygon","coordinates":"#);
    encode_multi_polygon_inner(geom, out);
    out.push(b'}');
}

fn encode_multi_polygon_inner(geom: &impl MultiPolygonTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    let num_polygons = geom.num_polygons();
    for (idx, polygon) in geom.polygons().enumerate() {
        encode_polygon_rings(&polygon, out);
        if idx < num_polygons - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use geoarrow_array::test::multipolygon::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_multi_polygon() {
        let mut encoder = MultiPolygonEncoder::new(array(CoordType::Separated, Dimension::XY));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"MultiPolygon","coordinates":[[[[30,10],[40,40],[20,40],[10,20],[30,10]]]]}"#;
        assert_eq!(s, expected);

        geojson::Geometry::from_str(expected).expect("Should be valid GeoJSON");
    }
}
