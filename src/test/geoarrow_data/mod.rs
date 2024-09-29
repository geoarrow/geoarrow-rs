pub mod util;

use crate::array::*;
use crate::test::geoarrow_data::util::read_geometry_column;

macro_rules! geoarrow_data_impl {
    ($fn_name:ident, $file_part:tt, $return_type:ty) => {
        pub(crate) fn $fn_name() -> $return_type {
            let path = format!("fixtures/geoarrow-data/example/example-{}.arrow", $file_part);
            let geometry_dyn_column = read_geometry_column(&path);
            geometry_dyn_column.as_ref().try_into().unwrap()
        }
    };
}

// Point
geoarrow_data_impl!(example_point_interleaved, "point-interleaved", PointArray<2>);
geoarrow_data_impl!(example_point_separated, "point", PointArray<2>);
geoarrow_data_impl!(example_point_wkb, "point-wkb", WKBArray<i64>);

// LineString
geoarrow_data_impl!(example_linestring_interleaved, "linestring-interleaved", LineStringArray<2>);
geoarrow_data_impl!(example_linestring_separated, "linestring", LineStringArray<2>);
geoarrow_data_impl!(example_linestring_wkb, "linestring-wkb", WKBArray<i64>);

// Polygon
geoarrow_data_impl!(example_polygon_interleaved, "polygon-interleaved", PolygonArray<2>);
geoarrow_data_impl!(example_polygon_separated, "polygon", PolygonArray<2>);
geoarrow_data_impl!(example_polygon_wkb, "polygon-wkb", WKBArray<i64>);

// MultiPoint
geoarrow_data_impl!(example_multipoint_interleaved, "multipoint-interleaved", MultiPointArray<2>);
geoarrow_data_impl!(example_multipoint_separated, "multipoint", MultiPointArray<2>);
geoarrow_data_impl!(example_multipoint_wkb, "multipoint-wkb", WKBArray<i64>);

// MultiLineString
geoarrow_data_impl!(example_multilinestring_interleaved, "multilinestring-interleaved", MultiLineStringArray<2>);
geoarrow_data_impl!(example_multilinestring_separated, "multilinestring", MultiLineStringArray<2>);
geoarrow_data_impl!(example_multilinestring_wkb, "multilinestring-wkb", WKBArray<i64>);

// MultiPolygon
geoarrow_data_impl!(example_multipolygon_interleaved, "multipolygon-interleaved", MultiPolygonArray<2>);
geoarrow_data_impl!(example_multipolygon_separated, "multipolygon", MultiPolygonArray<2>);
geoarrow_data_impl!(example_multipolygon_wkb, "multipolygon-wkb", WKBArray<i64>);
