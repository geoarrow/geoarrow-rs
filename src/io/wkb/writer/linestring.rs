use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{LineStringArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::{CoordTrait, LineStringTrait};
use crate::io::wkb::reader::geometry::Endianness;
use crate::trait_::GeometryArrayTrait;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBLineString
pub fn line_string_wkb_size(geom: &impl LineStringTrait) -> usize {
    1 + 4 + 4 + (geom.num_coords() * 16)
}

/// Write a LineString geometry to a Writer encoded as WKB
pub fn write_line_string_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl LineStringTrait<T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 2
    writer.write_u32::<LittleEndian>(2).unwrap();

    // numPoints
    writer
        .write_u32::<LittleEndian>(geom.num_coords().try_into().unwrap())
        .unwrap();

    for coord_idx in 0..geom.num_coords() {
        let coord = geom.coord(coord_idx).unwrap();
        writer.write_f64::<LittleEndian>(coord.x()).unwrap();
        writer.write_f64::<LittleEndian>(coord.y()).unwrap();
    }

    Ok(())
}

impl<A: OffsetSizeTrait, B: OffsetSizeTrait> From<&LineStringArray<A>> for WKBArray<B> {
    fn from(value: &LineStringArray<A>) -> Self {
        let mut offsets: OffsetsBuilder<B> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(line_string_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_line_string_as_wkb(&mut writer, &geom).unwrap();
            }

            writer.into_inner()
        };

        let binary_arr =
            GenericBinaryArray::new(offsets.into(), values.into(), value.nulls().cloned());
        WKBArray::new(binary_arr)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::linestring::{ls0, ls1};

    #[test]
    fn round_trip() {
        let orig_arr: LineStringArray<i32> = vec![Some(ls0()), Some(ls1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: LineStringArray<i32> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }

    // // TODO: parsing WKBArray<i64> into LineStringArray<i32> not yet implemented
    // fn round_trip_to_i64() {
    //     let orig_arr: LineStringArray<i32> = vec![Some(ls0()), Some(ls1()), None].into();
    //     let wkb_arr: WKBArray<i64> = (&orig_arr).into();
    //     let new_arr: LineStringArray<i32> = wkb_arr.try_into().unwrap();

    //     assert_eq!(orig_arr, new_arr);
    // }
}
