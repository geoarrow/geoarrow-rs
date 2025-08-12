use std::io::Write;

use arrow_json::Encoder;
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::RectArray;

// An [Encoder] for [RectArray].
pub struct RectEncoder(RectArray);

impl RectEncoder {
    pub fn new(array: RectArray) -> Self {
        Self(array)
    }
}

impl Encoder for RectEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let rect = self.0.value(idx).unwrap();
        encode_rect(&rect, out);
    }
}

fn encode_rect(rect: &impl RectTrait<T = f64>, out: &mut Vec<u8>) {
    out.extend(br#"{"type":"Polygon","coordinates":["#);

    // Get the min and max coordinates
    let min_coord = rect.min();
    let max_coord = rect.max();

    // Create exterior ring coordinates: [min_x, min_y], [min_x, max_y], [max_x, max_y], [max_x, min_y], [min_x, min_y]
    out.push(b'[');

    // Bottom-left: [min_x, min_y]
    out.push(b'[');
    write!(out, "{}", min_coord.x()).unwrap();
    out.push(b',');
    write!(out, "{}", min_coord.y()).unwrap();
    out.push(b']');
    out.push(b',');

    // Top-left: [min_x, max_y]
    out.push(b'[');
    write!(out, "{}", min_coord.x()).unwrap();
    out.push(b',');
    write!(out, "{}", max_coord.y()).unwrap();
    out.push(b']');
    out.push(b',');

    // Top-right: [max_x, max_y]
    out.push(b'[');
    write!(out, "{}", max_coord.x()).unwrap();
    out.push(b',');
    write!(out, "{}", max_coord.y()).unwrap();
    out.push(b']');
    out.push(b',');

    // Bottom-right: [max_x, min_y]
    out.push(b'[');
    write!(out, "{}", max_coord.x()).unwrap();
    out.push(b',');
    write!(out, "{}", min_coord.y()).unwrap();
    out.push(b']');
    out.push(b',');

    // Close the ring: [min_x, min_y]
    out.push(b'[');
    write!(out, "{}", min_coord.x()).unwrap();
    out.push(b',');
    write!(out, "{}", min_coord.y()).unwrap();
    out.push(b']');

    out.push(b']'); // Close exterior ring
    out.push(b']'); // Close coordinates array
    out.push(b'}'); // Close geometry object
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use geo_types::{Rect, coord};
    use geoarrow_array::builder::RectBuilder;
    use geoarrow_schema::{BoxType, Dimension};

    use super::*;

    #[test]
    fn encode_rect() {
        // Create a simple rect array for testing
        let geoms = [Rect::new(
            coord! { x: 10., y: 20. },
            coord! { x: 30., y: 10. },
        )];
        let typ = BoxType::new(Dimension::XY, Default::default());
        let array = RectBuilder::from_rects(geoms.iter(), typ).finish();

        let mut encoder = RectEncoder::new(array);

        let mut out = vec![];
        encoder.encode(0, &mut out);
        let s = String::from_utf8(out).unwrap();
        // The rect bounds should create a polygon: min_x=10, min_y=10, max_x=30, max_y=20
        let expected =
            r#"{"type":"Polygon","coordinates":[[[10,10],[10,20],[30,20],[30,10],[10,10]]]}"#;
        assert_eq!(s, expected);

        geojson::Geometry::from_str(expected).expect("Should be valid GeoJSON");
    }
}
