use crate::array::{LineStringArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::{CoordTrait, LineStringTrait};
use crate::io::wkb::reader::geometry::Endianness;
use crate::trait_::GeometryArrayTrait;
use arrow2::array::BinaryArray;
use arrow2::datatypes::DataType;
use arrow2::offset::Offsets;
use arrow2::types::Offset;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBLineString
pub fn linestring_wkb_size<'a>(geom: impl LineStringTrait<'a>) -> usize {
    1 + 4 + 4 + (geom.num_coords() * 16)
}

/// Write a Point geometry to a Writer encoded as WKB
pub fn write_line_string_as_wkb<'a, W: Write>(
    mut writer: W,
    geom: impl LineStringTrait<'a, T = f64>,
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

// TODO: decouple these two generics?
impl<O: Offset> From<&LineStringArray<O>> for WKBArray<O> {
    fn from(value: &LineStringArray<O>) -> Self {
        let mut offsets: Offsets<O> = Offsets::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(linestring_wkb_size(geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_line_string_as_wkb(&mut writer, geom).unwrap();
            }

            writer.into_inner()
        };

        let data_type = match O::IS_LARGE {
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
    use crate::test::linestring::{ls0, ls1};

    #[test]
    fn round_trip() {
        let orig_arr: LineStringArray<i32> = vec![Some(ls0()), Some(ls1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: LineStringArray<i32> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
