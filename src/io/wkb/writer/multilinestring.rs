use crate::array::{MultiLineStringArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::MultiLineStringTrait;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::writer::linestring::{line_string_wkb_size, write_line_string_as_wkb};
use crate::trait_::GeometryArrayTrait;
use arrow2::array::BinaryArray;
use arrow2::datatypes::DataType;
use arrow2::offset::Offsets;
use arrow2::types::Offset;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBMultiLineString
pub fn multi_line_string_wkb_size<'a>(geom: &impl MultiLineStringTrait<'a>) -> usize {
    let mut sum = 1 + 4 + 4;
    for line_string_idx in 0..geom.num_lines() {
        let line_string = geom.line(line_string_idx).unwrap();
        sum += line_string_wkb_size(&line_string);
    }

    sum
}

/// Write a MultiLineString geometry to a Writer encoded as WKB
pub fn write_multi_line_string_as_wkb<'a, W: Write>(
    mut writer: W,
    geom: impl MultiLineStringTrait<'a, T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 5
    writer.write_u32::<LittleEndian>(5).unwrap();

    // numPoints
    writer
        .write_u32::<LittleEndian>(geom.num_lines().try_into().unwrap())
        .unwrap();

    for line_string_idx in 0..geom.num_lines() {
        let line_string = geom.line(line_string_idx).unwrap();
        write_line_string_as_wkb(&mut writer, line_string).unwrap();
    }

    Ok(())
}

impl<A: Offset, B: Offset> From<&MultiLineStringArray<A>> for WKBArray<B> {
    fn from(value: &MultiLineStringArray<A>) -> Self {
        let mut offsets: Offsets<B> = Offsets::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets
                    .try_push_usize(multi_line_string_wkb_size(&geom))
                    .unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_multi_line_string_as_wkb(&mut writer, geom).unwrap();
            }

            writer.into_inner()
        };

        let data_type = match B::IS_LARGE {
            true => DataType::LargeBinary,
            false => DataType::Binary,
        };

        let binary_arr = BinaryArray::new(
            data_type,
            offsets.into(),
            values.into(),
            value.validity().cloned(),
        );
        WKBArray::new(binary_arr)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multilinestring::{ml0, ml1};

    #[test]
    fn round_trip() {
        let orig_arr: MultiLineStringArray<i32> = vec![Some(ml0()), Some(ml1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiLineStringArray<i32> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
