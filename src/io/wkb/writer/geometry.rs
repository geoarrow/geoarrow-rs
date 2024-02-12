use arrow_array::{GenericBinaryArray, OffsetSizeTrait};

use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{MixedGeometryArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::io::wkb::writer::{
    geometry_collection_wkb_size, line_string_wkb_size, multi_line_string_wkb_size,
    multi_point_wkb_size, multi_polygon_wkb_size, polygon_wkb_size, write_line_string_as_wkb,
    write_multi_line_string_as_wkb, write_multi_point_as_wkb, write_multi_polygon_as_wkb,
    write_point_as_wkb, write_polygon_as_wkb, POINT_WKB_SIZE,
};
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::GeometryArrayTrait;
use std::io::{Cursor, Write};

/// The byte length of a Geometry
pub fn geometry_wkb_size(geom: &impl GeometryTrait) -> usize {
    use GeometryType::*;
    match geom.as_type() {
        Point(_) => POINT_WKB_SIZE,
        LineString(ls) => line_string_wkb_size(ls),
        Polygon(p) => polygon_wkb_size(p),
        MultiPoint(mp) => multi_point_wkb_size(mp),
        MultiLineString(ml) => multi_line_string_wkb_size(ml),
        MultiPolygon(mp) => multi_polygon_wkb_size(mp),
        GeometryCollection(gc) => geometry_collection_wkb_size(gc),
        Rect(_) => todo!(),
    }
}

/// Write a Geometry to a Writer encoded as WKB
pub fn write_geometry_as_wkb<W: Write>(
    writer: W,
    geom: &impl GeometryTrait<T = f64>,
) -> Result<()> {
    use GeometryType::*;
    match geom.as_type() {
        Point(p) => write_point_as_wkb(writer, p),
        LineString(ls) => write_line_string_as_wkb(writer, ls),
        Polygon(p) => write_polygon_as_wkb(writer, p),
        MultiPoint(mp) => write_multi_point_as_wkb(writer, mp),
        MultiLineString(ml) => write_multi_line_string_as_wkb(writer, ml),
        MultiPolygon(mp) => write_multi_polygon_as_wkb(writer, mp),
        GeometryCollection(_gc) => {
            todo!()
            // error[E0275]: overflow evaluating the requirement `&mut std::io::Cursor<std::vec::Vec<u8>>: std::io::Write`
            // https://stackoverflow.com/a/31197781/7319250
            // write_geometry_collection_as_wkb(writer, gc)
        }
        Rect(_) => todo!(),
        // _ => todo!(),
    }
}

impl<A: OffsetSizeTrait, B: OffsetSizeTrait> From<&MixedGeometryArray<A>> for WKBArray<B> {
    fn from(value: &MixedGeometryArray<A>) -> Self {
        let mut offsets: OffsetsBuilder<B> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(geometry_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_geometry_as_wkb(&mut writer, &geom).unwrap();
            }

            writer.into_inner()
        };

        let binary_arr =
            GenericBinaryArray::new(offsets.into(), values.into(), value.nulls().cloned());
        WKBArray::new(binary_arr, value.metadata())
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
