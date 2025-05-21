use arrow_json::Encoder;
use geo_traits::LineStringTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::LineStringArray;

use crate::encoders::point::encode_coord;

pub(crate) struct LineStringEncoder(pub(crate) LineStringArray);

impl Encoder for LineStringEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_line_string(&geom, out);
    }
}

/// Encode a LineString geometry including the `type: LineString` header
fn encode_line_string(geom: &impl LineStringTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type": "LineString", "coordinates":"#);
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
