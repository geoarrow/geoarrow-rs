//! A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification,
//! plus algorithms implemented on and returning these GeoArrow arrays.

pub use trait_::GeometryArrayTrait;

pub mod algorithm;
pub mod array;
pub mod chunked_array;
pub mod datatypes;
pub mod error;
pub mod geo_traits;
pub mod io;
pub mod scalar;
pub mod table;
#[cfg(test)]
pub(crate) mod test;
pub mod trait_;
mod util;

#[cfg(test)]
mod test2 {
    use crate::array::WKBBuilder;
    use crate::io::wkb::reader::WKBGeometry;
    use crate::trait_::GeometryArrayAccessor;

    #[test]
    fn test_wkb_to_bbox() {
        use crate::algorithm::native::bounding_rect::BoundingRect;
        use crate::test::polygon::p0;

        // A builder for a columnar WKB arrays
        let mut wkb_builder = WKBBuilder::<i32>::new();
        // Add a geo polygon to the WKB array
        // This uses geo-traits to serialize to WKB and adds the binary to the array
        wkb_builder.push_polygon(Some(&p0()));

        // Finish the builder, creating an array of logical length 1.
        let wkb_arr = wkb_builder.finish();

        // Access the WKB scalar at position 0
        // This is a reference onto the array. At this point the WKB is just a "blob" with no other
        // information.
        let wkb_scalar = wkb_arr.value(0);

        // This is a "parsed" WKB object. The [WKBGeometry] type is an enum over each geometry
        // type. WKBGeometry itself implements GeometryTrait but we need to unpack this to a
        // WKBPolygon to access the object that has the PolygonTrait impl
        let wkb_object = wkb_scalar.to_wkb_object();

        // This is a WKBPolygon. It's already been scanned to know where each ring starts and ends,
        // so it's O(1) from this point to access any single coordinate.
        let wkb_polygon = match wkb_object {
            WKBGeometry::Polygon(wkb_polygon) => wkb_polygon,
            _ => unreachable!(),
        };

        // Add this wkb object directly into the BoundingRect
        let mut bounding_rect = BoundingRect::new();
        bounding_rect.add_polygon(&wkb_polygon);

        assert_eq!(bounding_rect.minx, -111.);
        assert_eq!(bounding_rect.miny, 41.);
        assert_eq!(bounding_rect.maxx, -104.);
        assert_eq!(bounding_rect.maxy, 45.);
    }

    #[test]
    fn test_wkb_to_frechet() {
        use crate::algorithm::geo::{
            FrechetDistance as OtherFrechetDistance, FrechetDistanceLineString,
        };
        use crate::test::linestring::{ls0, ls1};

        let mut wkb_builder = WKBBuilder::<i32>::new();
        wkb_builder.push_line_string(Some(&ls0()));
        let wkb_arr = wkb_builder.finish();
        let wkb_scalar = wkb_arr.value(0);
        let wkb_object = wkb_scalar.to_wkb_object();
        let wkb_ls0 = match wkb_object {
            WKBGeometry::LineString(wkb) => wkb,
            _ => unreachable!(),
        };

        let mut wkb_builder = WKBBuilder::<i32>::new();
        wkb_builder.push_line_string(Some(&ls1()));
        let wkb_arr = wkb_builder.finish();
        let wkb_scalar = wkb_arr.value(0);
        let wkb_object = wkb_scalar.to_wkb_object();
        let wkb_ls1 = match wkb_object {
            WKBGeometry::LineString(wkb) => wkb,
            _ => unreachable!(),
        };

        let expected = 5.656854249492381;

        // passes
        use crate::io::geo::line_string_to_geo;
        use geo::FrechetDistance;
        let distance = line_string_to_geo(&wkb_ls0).frechet_distance(&line_string_to_geo(&wkb_ls1));
        approx::assert_relative_eq!(distance, expected);

        // DOES NOT COMPILE:
        //     error[E0599]: no method named `frechet_distance` found for struct `array::binary::array::WKBArray` in the current scope
        //
        // let distance = wkb_ls0.frechet_distance(&wkb_ls1);
        // assert_eq!(distance, expected);

        // DOES NOT COMPILE:
        //    error[E0599]: no method named `frechet_distance` found for struct `array::binary::array::WKBArray` in the current scope
        //
        // let distance = wkb_arr.frechet_distance(&wkb_ls1);
        // assert_eq!(distance, expected);
    }
}
