use arrow_json::Encoder;
use geo_traits::GeometryTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::GeometryArray;

use crate::encoders::geometrycollection::encode_geometry_collection;
use crate::encoders::linestring::encode_line_string;
use crate::encoders::multilinestring::encode_multi_line_string;
use crate::encoders::multipoint::encode_multi_point;
use crate::encoders::multipolygon::encode_multi_polygon;
use crate::encoders::point::encode_point;
use crate::encoders::polygon::encode_polygon;

pub(crate) struct GeometryEncoder(GeometryArray);

impl GeometryEncoder {
    pub(crate) fn new(array: GeometryArray) -> Self {
        Self(array)
    }
}

impl Encoder for GeometryEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let geom = self.0.value(idx).unwrap();
        encode_geometry(&geom, out);
    }
}

/// Encode a Geometry geometry including the `type: Geometry` header
pub(crate) fn encode_geometry(geom: &impl GeometryTrait<T = f64>, out: &mut Vec<u8>) {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(geom) => encode_point(geom, out),
        LineString(geom) => {
            encode_line_string(geom, out);
        }
        Polygon(geom) => encode_polygon(geom, out),
        MultiPoint(geom) => encode_multi_point(geom, out),
        MultiLineString(geom) => encode_multi_line_string(geom, out),
        MultiPolygon(geom) => encode_multi_polygon(geom, out),
        GeometryCollection(geom) => encode_geometry_collection(geom, out),
        _ => todo!(),
    }
}

#[cfg(test)]
mod test {
    use geoarrow_array::test::geometry::array;
    use geoarrow_schema::CoordType;

    use super::*;

    #[test]
    fn encode_geometry() {
        let mut encoder = GeometryEncoder::new(array(CoordType::Separated, false));

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        let expected = r#"{"type":"Point","coordinates":[30,10]}"#;
        assert_eq!(s, expected);
    }
}
