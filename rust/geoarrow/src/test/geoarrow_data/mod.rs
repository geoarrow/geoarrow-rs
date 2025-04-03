pub mod util;

use crate::array::*;
use crate::test::geoarrow_data::util::read_geometry_column;
use geoarrow_schema::Dimension;

macro_rules! geoarrow_data_impl {
    ($fn_name:ident, $file_part:tt, $return_type:ty) => {
        pub(crate) fn $fn_name() -> $return_type {
            let path = format!(
                "fixtures/geoarrow-data/example/example-{}.arrow",
                $file_part
            );
            let geometry_dyn_column = read_geometry_column(&path);
            (geometry_dyn_column.as_ref(), Dimension::XY)
                .try_into()
                .unwrap()
        }
    };
    ($fn_name:ident, $file_part:tt, $return_type:ty, "WKB") => {
        pub(crate) fn $fn_name() -> $return_type {
            let path = format!(
                "fixtures/geoarrow-data/example/example-{}.arrow",
                $file_part
            );
            let geometry_dyn_column = read_geometry_column(&path);
            geometry_dyn_column.as_ref().try_into().unwrap()
        }
    };
}

// Point
geoarrow_data_impl!(example_point_interleaved, "point-interleaved", PointArray);
geoarrow_data_impl!(example_point_separated, "point", PointArray);
geoarrow_data_impl!(example_point_wkb, "point-wkb", WKBArray<i64>, "WKB");

// LineString
geoarrow_data_impl!(
    example_linestring_interleaved,
    "linestring-interleaved",
    LineStringArray
);
geoarrow_data_impl!(example_linestring_separated, "linestring", LineStringArray);
geoarrow_data_impl!(
    example_linestring_wkb,
    "linestring-wkb",
    WKBArray<i64>,
    "WKB"
);

// Polygon
geoarrow_data_impl!(
    example_polygon_interleaved,
    "polygon-interleaved",
    PolygonArray
);
geoarrow_data_impl!(example_polygon_separated, "polygon", PolygonArray);
geoarrow_data_impl!(example_polygon_wkb, "polygon-wkb", WKBArray<i64>, "WKB");

// MultiPoint
geoarrow_data_impl!(
    example_multipoint_interleaved,
    "multipoint-interleaved",
    MultiPointArray
);
geoarrow_data_impl!(example_multipoint_separated, "multipoint", MultiPointArray);
geoarrow_data_impl!(
    example_multipoint_wkb,
    "multipoint-wkb",
    WKBArray<i64>,
    "WKB"
);

// MultiLineString
geoarrow_data_impl!(
    example_multilinestring_interleaved,
    "multilinestring-interleaved",
    MultiLineStringArray
);
geoarrow_data_impl!(
    example_multilinestring_separated,
    "multilinestring",
    MultiLineStringArray
);
geoarrow_data_impl!(
    example_multilinestring_wkb,
    "multilinestring-wkb",
    WKBArray<i64>,
    "WKB"
);

// MultiPolygon
geoarrow_data_impl!(
    example_multipolygon_interleaved,
    "multipolygon-interleaved",
    MultiPolygonArray
);
geoarrow_data_impl!(
    example_multipolygon_separated,
    "multipolygon",
    MultiPolygonArray
);
geoarrow_data_impl!(
    example_multipolygon_wkb,
    "multipolygon-wkb",
    WKBArray<i64>,
    "WKB"
);
