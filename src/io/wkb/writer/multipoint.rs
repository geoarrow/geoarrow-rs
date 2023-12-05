use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{MultiPointArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::MultiPointTrait;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::writer::point::{write_point_as_wkb, POINT_WKB_SIZE};
use crate::trait_::GeometryArrayTrait;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBMultiPoint
pub fn multi_point_wkb_size(geom: &impl MultiPointTrait) -> usize {
    1 + 4 + 4 + (geom.num_points() * POINT_WKB_SIZE)
}

/// Write a MultiPoint geometry to a Writer encoded as WKB
pub fn write_multi_point_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl MultiPointTrait<T = f64>,
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
        write_point_as_wkb(&mut writer, &point).unwrap();
    }

    Ok(())
}

impl<A: OffsetSizeTrait, B: OffsetSizeTrait> From<&MultiPointArray<A>> for WKBArray<B> {
    fn from(value: &MultiPointArray<A>) -> Self {
        let mut offsets: OffsetsBuilder<B> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(multi_point_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_multi_point_as_wkb(&mut writer, &geom).unwrap();
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
    use crate::test::multipoint::{mp0, mp1};

    #[test]
    fn round_trip() {
        let orig_arr: MultiPointArray<i32> = vec![Some(mp0()), Some(mp1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiPointArray<i32> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
