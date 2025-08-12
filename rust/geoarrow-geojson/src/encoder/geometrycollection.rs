use arrow_json::Encoder;
use geo_traits::GeometryCollectionTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::GeometryCollectionArray;

use crate::encoder::geometry::encode_geometry;

// An [Encoder] for [GeometryCollectionArray].
pub struct GeometryCollectionEncoder(GeometryCollectionArray);

impl GeometryCollectionEncoder {
    pub fn new(array: GeometryCollectionArray) -> Self {
        Self(array)
    }
}

impl Encoder for GeometryCollectionEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_geometry_collection(&geom, out);
    }
}

/// Encode a GeometryCollection geometry including the `type: GeometryCollection` header
pub(crate) fn encode_geometry_collection(
    geom: &impl GeometryCollectionTrait<T = f64>,
    out: &mut Vec<u8>,
) {
    out.extend(br#"{"type":"GeometryCollection","geometries":"#);
    encode_geometries(geom, out);
    out.push(b'}');
}

/// Encode the coordinates of a LineString geometry
fn encode_geometries(gc: &impl GeometryCollectionTrait<T = f64>, out: &mut Vec<u8>) {
    out.push(b'[');
    let num_geometries = gc.num_geometries();
    for (idx, geom) in gc.geometries().enumerate() {
        encode_geometry(&geom, out);
        if idx < num_geometries - 1 {
            out.push(b',');
        }
    }
    out.push(b']');
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use geoarrow_array::test::geometrycollection::array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn encode_geometry_collection() {
        let mut encoder =
            GeometryCollectionEncoder::new(array(CoordType::Separated, Dimension::XY, false));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[30,10]}]}"#;
        assert_eq!(s, expected);
        // println!("{}", s);

        geojson::Geometry::from_str(expected).expect("Should be valid GeoJSON");
    }
}
