use arrow_json::Encoder;
use geo_traits::{LineStringTrait, PolygonTrait};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::PolygonArray;

use crate::encoders::linestring::encode_coords;

pub(crate) struct PolygonEncoder(PolygonArray);

impl PolygonEncoder {
    pub(crate) fn new(array: PolygonArray) -> Self {
        Self(array)
    }
}

impl Encoder for PolygonEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_polygon(&geom, out);
    }
}

pub(crate) fn encode_polygon(geom: &impl PolygonTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type":"Polygon","coordinates":"#);
    encode_polygon_rings(geom, out);
    out.push(b'}');
}

pub(crate) fn encode_polygon_rings(geom: &impl PolygonTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    if let Some(exterior) = geom.exterior() {
        encode_coords(exterior.coords(), out);
    }

    let num_interiors = geom.num_interiors();
    if num_interiors > 0 {
        out.push(b',');
    }

    for (idx, interior) in geom.interiors().enumerate() {
        encode_coords(interior.coords(), out);
        if idx < num_interiors - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use geoarrow_array::test::polygon::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_polygon() {
        let mut encoder = PolygonEncoder::new(array(CoordType::Separated, Dimension::XY));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected =
            r#"{"type":"Polygon","coordinates":[[[30,10],[40,40],[20,40],[10,20],[30,10]]]}"#;
        assert_eq!(s, expected);
    }
}
