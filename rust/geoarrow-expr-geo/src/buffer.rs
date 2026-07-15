use geo::Buffer;
use geoarrow_array::array::MultiPolygonArray;
use geoarrow_array::builder::MultiPolygonBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};

use crate::util::to_geo::geometry_to_geo;

/// Buffer each geometry by `distance`, producing a `MultiPolygonArray`.
///
/// A positive distance grows the geometry outward, a negative distance shrinks
/// it inward (for areal inputs). Buffering uses `geo`'s default rounded joins
/// and end caps; the styled `buffer_with_style` variants are not exposed here.
///
/// The output is always a 2D `MultiPolygon`. Any Z/M on the input is dropped:
/// the buffer boundary is made of newly generated vertices that have no source
/// Z/M to carry, so preserving those dimensions is not meaningful. Null input
/// rows produce null output rows and the row count is preserved.
pub fn buffer(
    array: &dyn GeoArrowArray,
    distance: f64,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    downcast_geoarrow_array!(array, _buffer_impl, distance, coord_type)
}

fn _buffer_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    distance: f64,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    let typ = MultiPolygonType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = MultiPolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let buffered = geo_geom.buffer(distance);
            builder.push_multi_polygon(Some(&buffered))?;
        } else {
            builder.push_multi_polygon(None::<geo::MultiPolygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use geo::Area;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PolygonArray;
    use geoarrow_array::builder::{PolygonBuilder, WkbBuilder};
    use geoarrow_schema::{CoordType, Dimension, PolygonType, WkbType};

    use super::*;
    use crate::util::to_geo::geometry_to_geo;

    // The unit square (area 1.0), used as a known-area input across tests.
    fn unit_square() -> geo::Polygon {
        geo::Polygon::new(
            geo::LineString::from(vec![
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            vec![],
        )
    }

    // A single-row PolygonArray holding the unit square, plus a trailing null
    // row so null-passthrough and row-count preservation are exercised.
    fn unit_square_array() -> PolygonArray {
        let typ = PolygonType::new(Dimension::XY, Default::default());
        let mut builder = PolygonBuilder::new(typ);
        builder.push_polygon(Some(&unit_square())).unwrap();
        builder.push_polygon(None::<&geo::Polygon>).unwrap();
        builder.finish()
    }

    // Sum the unsigned area of every non-null row of a MultiPolygon result.
    fn total_area(result: &MultiPolygonArray) -> f64 {
        let mut sum = 0.0;
        for geom in result.iter().flatten() {
            let geo_geom = geometry_to_geo(&geom.unwrap()).unwrap();
            sum += geo_geom.unsigned_area();
        }
        sum
    }

    #[test]
    fn buffer_grows_area() {
        let arr = unit_square_array();
        let result = buffer(&arr, 0.5, CoordType::Interleaved).unwrap();

        // Row count preserved.
        assert_eq!(result.len(), arr.len());

        // Buffering the unit square (area 1.0) outward by 0.5 must enlarge it.
        assert!(
            total_area(&result) > 1.0,
            "buffered area should exceed the 1.0 source area"
        );
    }

    #[test]
    fn buffer_null_passthrough() {
        let arr = unit_square_array();
        let result = buffer(&arr, 0.5, CoordType::Separated).unwrap();

        assert!(
            result.iter().next().unwrap().is_some(),
            "row 0 should be present"
        );
        assert!(
            result.iter().nth(1).unwrap().is_none(),
            "row 1 (null input) should stay null"
        );
    }

    // Drive the WKB parse path (mirrors length.rs's test_wkb_* style), the same
    // byte path the eventual wasm binding feeds the kernel. Buffering the unit
    // square marshaled through WKB must still enlarge its area.
    #[test]
    fn buffer_wkb_polygon() {
        let mut wkb_builder: WkbBuilder<i32> = WkbBuilder::new(WkbType::new(Default::default()));
        wkb_builder.push_geometry(Some(&unit_square())).unwrap();
        let wkb_array = wkb_builder.finish();

        let result = buffer(&wkb_array, 0.5, CoordType::Interleaved).unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            total_area(&result) > 1.0,
            "buffered WKB unit square should exceed the 1.0 source area"
        );
    }
}
