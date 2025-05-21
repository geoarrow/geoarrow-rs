use arrow_json::Encoder;
use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::PointArray;
use std::io::Write;

pub(crate) struct PointEncoder(pub(crate) PointArray);

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
