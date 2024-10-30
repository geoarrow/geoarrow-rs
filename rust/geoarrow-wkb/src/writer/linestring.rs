use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{LineStringArray, WKBArray};
use crate::error::WKBResult;
use crate::common::WKBType;
use crate::reader::Endianness;
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use byteorder::{LittleEndian, WriteBytesExt};
use geo_traits::{CoordTrait, LineStringTrait};
use std::io::{Cursor, Write};

/// The byte length of a WKBLineString
pub fn line_string_wkb_size(geom: &impl LineStringTrait) -> usize {
    let header = 1 + 4 + 4;
    let each_coord = geom.dim().size() * 8;
    let all_coords = geom.num_coords() * each_coord;
    header + all_coords
}

/// Write a LineString geometry to a Writer encoded as WKB
pub fn write_line_string_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl LineStringTrait<T = f64>,
) -> WKBResult<()> {
    use geo_traits::Dimensions;

    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    match geom.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => {
            writer
                .write_u32::<LittleEndian>(WKBType::LineString.into())
                .unwrap();
        }
        Dimensions::Xyz | Dimensions::Unknown(3) => {
            writer
                .write_u32::<LittleEndian>(WKBType::LineStringZ.into())
                .unwrap();
        }
        _ => panic!(),
    }

    // numPoints
    writer
        .write_u32::<LittleEndian>(geom.num_coords().try_into().unwrap())
        .unwrap();

    for coord in geom.coords() {
        writer.write_f64::<LittleEndian>(coord.x()).unwrap();
        writer.write_f64::<LittleEndian>(coord.y()).unwrap();

        if geom.dim().size() == 3 {
            writer
                .write_f64::<LittleEndian>(coord.nth_unchecked(2))
                .unwrap();
        }
    }

    Ok(())
}

impl<O: OffsetSizeTrait, const D: usize> From<&LineStringArray<D>> for WKBArray<O> {
    fn from(value: &LineStringArray<D>) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

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

        let binary_arr = GenericBinaryArray::new(
            offsets.into(),
            Buffer::from_vec(values),
            value.nulls().cloned(),
        );
        WKBArray::new(binary_arr, value.metadata())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::linestring::{ls0, ls1};

    #[test]
    fn round_trip() {
        let orig_arr: LineStringArray<2> = vec![Some(ls0()), Some(ls1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: LineStringArray<2> = wkb_arr.try_into().unwrap();

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
