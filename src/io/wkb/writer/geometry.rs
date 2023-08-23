use crate::error::Result;
use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::io::wkb::writer::linestring::{line_string_wkb_size, write_line_string_as_wkb};
use crate::io::wkb::writer::multilinestring::{
    multi_line_string_wkb_size, write_multi_line_string_as_wkb,
};
use crate::io::wkb::writer::multipoint::{multi_point_wkb_size, write_multi_point_as_wkb};
use crate::io::wkb::writer::multipolygon::{multi_polygon_wkb_size, write_multi_polygon_as_wkb};
use crate::io::wkb::writer::point::{write_point_as_wkb, POINT_WKB_SIZE};
use crate::io::wkb::writer::polygon::{polygon_wkb_size, write_polygon_as_wkb};
use std::io::Write;

/// The byte length of a Geometry
pub fn geometry_wkb_size<'a>(geom: impl GeometryTrait<'a> + 'a) -> usize {
    use GeometryType::*;
    match geom.as_type() {
        Point(_) => POINT_WKB_SIZE,
        LineString(ls) => line_string_wkb_size(ls),
        Polygon(p) => polygon_wkb_size(p),
        MultiPoint(mp) => multi_point_wkb_size(mp),
        MultiLineString(ml) => multi_line_string_wkb_size(ml),
        MultiPolygon(mp) => multi_polygon_wkb_size(mp),
        _ => todo!(),
    }
}

/// Write a Geometry to a Writer encoded as WKB
pub fn write_geometry_as_wkb<'a, W: Write>(
    writer: W,
    geom: impl GeometryTrait<'a, T = f64> + 'a,
) -> Result<()> {
    use GeometryType::*;
    match geom.as_type() {
        Point(p) => write_point_as_wkb(writer, p),
        LineString(ls) => write_line_string_as_wkb(writer, ls),
        Polygon(p) => write_polygon_as_wkb(writer, p),
        MultiPoint(mp) => write_multi_point_as_wkb(writer, mp),
        MultiLineString(ml) => write_multi_line_string_as_wkb(writer, ml),
        MultiPolygon(mp) => write_multi_polygon_as_wkb(writer, mp),
        _ => todo!(),
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::multilinestring::{ml0, ml1};

//     #[test]
//     fn round_trip() {
//         let orig_arr: MultiLineStringArray<i32> = vec![Some(ml0()), Some(ml1()), None].into();
//         let wkb_arr: WKBArray<i32> = (&orig_arr).into();
//         let new_arr: MultiLineStringArray<i32> = wkb_arr.try_into().unwrap();

//         assert_eq!(orig_arr, new_arr);
//     }
// }
