use arrow_json::Encoder;
use geo_traits::PolygonTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::PolygonArray;

use crate::encoders::linestring::encode_line_string_coords;

pub(crate) struct PolygonEncoder(pub(crate) PolygonArray);

impl Encoder for PolygonEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_polygon(&geom, out);
    }
}

pub(crate) fn encode_polygon(geom: &impl PolygonTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type": "Polygon", "coordinates":"#);
    encode_polygon_rings(geom, out);
    out.push(b'}');
}

pub(crate) fn encode_polygon_rings(geom: &impl PolygonTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    if let Some(exterior) = geom.exterior() {
        encode_line_string_coords(&exterior, out);
    }

    let num_interiors = geom.num_interiors();
    if num_interiors > 0 {
        out.push(b',');
    }

    for (idx, interior) in geom.interiors().enumerate() {
        encode_line_string_coords(&interior, out);
        if idx < num_interiors - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}
