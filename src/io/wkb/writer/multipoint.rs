use crate::array::{MultiPointArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::MultiPointTrait;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::writer::point::{write_point_as_wkb, POINT_WKB_SIZE};
use crate::trait_::GeometryArrayTrait;
use arrow2::array::BinaryArray;
use arrow2::datatypes::DataType;
use arrow2::offset::Offsets;
use arrow2::types::Offset;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBMultiPoint
pub fn multi_point_wkb_size<'a>(geom: &impl MultiPointTrait<'a>) -> usize {
    1 + 4 + 4 + (geom.num_points() * POINT_WKB_SIZE)
}

/// Write a MultiPoint geometry to a Writer encoded as WKB
pub fn write_multi_point_as_wkb<'a, W: Write>(
    mut writer: W,
    geom: impl MultiPointTrait<'a, T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 4
    writer.write_u32::<LittleEndian>(4).unwrap();

    // numPoints
    writer
        .write_u32::<LittleEndian>(geom.num_points().try_into().unwrap())
        .unwrap();

    for point_idx in 0..geom.num_points() {
        let point = geom.point(point_idx).unwrap();
        write_point_as_wkb(&mut writer, point).unwrap();
    }

    Ok(())
}

impl<A: Offset, B: Offset> From<&MultiPointArray<A>> for WKBArray<B> {
    fn from(value: &MultiPointArray<A>) -> Self {
        let mut offsets: Offsets<B> = Offsets::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(multi_point_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_multi_point_as_wkb(&mut writer, geom).unwrap();
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
    use crate::test::multipoint::{mp0, mp1};

    #[test]
    fn round_trip() {
        let orig_arr: MultiPointArray<i32> = vec![Some(mp0()), Some(mp1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiPointArray<i32> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
