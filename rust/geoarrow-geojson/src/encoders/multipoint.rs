use arrow_json::Encoder;
use geo_traits::MultiPointTrait;
use geoarrow_array::ArrayAccessor;
use geoarrow_array::array::MultiPointArray;

use crate::encoders::point::encode_coord;

pub(crate) struct MultiPointEncoder(pub(crate) MultiPointArray);

// impl Encoder for MultiPointEncoder {
//     fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
//         let geom = self.0.value(idx).unwrap();
//         encode_line_string(&geom, out);
//     }
// }

// /// Encode a MultiPoint geometry including the `type: MultiPoint` header
// pub(crate) fn encode_line_string(geom: &impl MultiPointTrait<T = f64>, out: &mut Vec<u8>) {
//     out.extend(br#"{"type": "MultiPoint", "coordinates":"#);
//     out.push(b'[');
//     let num_points = geom.num_points();
//     for (idx, coord) in geom.points().enumerate() {
//         encode_coord(&coord, out);
//         if idx < num_points - 1 {
//             out.push(b',');
//         }
//     }
//     out.push(b']');
//     out.push(b'}');
// }
