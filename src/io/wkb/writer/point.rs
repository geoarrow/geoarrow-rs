use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{PointArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::PointTrait;
use crate::io::wkb::common::WKBType;
use crate::io::wkb::reader::Endianness;
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::NativeArray;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBPoint
pub fn point_wkb_size(dim: usize) -> usize {
    let header = 1 + 4;
    let coords = dim * 8;
    header + coords
}

/// The byte length of a WKBPoint
pub fn point_wkb_size_const<const D: usize>() -> usize {
    let header = 1 + 4;
    let coords = D * 8;
    header + coords
}

/// Write a Point geometry to a Writer encoded as WKB
pub fn write_point_as_wkb<W: Write>(mut writer: W, geom: &impl PointTrait<T = f64>) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    match geom.dim() {
        2 => {
            writer
                .write_u32::<LittleEndian>(WKBType::Point.into())
                .unwrap();
        }
        3 => {
            writer
                .write_u32::<LittleEndian>(WKBType::PointZ.into())
                .unwrap();
        }
        _ => panic!(),
    }

    writer.write_f64::<LittleEndian>(geom.x()).unwrap();
    writer.write_f64::<LittleEndian>(geom.y()).unwrap();

    if geom.dim() == 3 {
        writer
            .write_f64::<LittleEndian>(geom.nth_unchecked(2))
            .unwrap();
    }

    Ok(())
}

impl<O: OffsetSizeTrait, const D: usize> From<&PointArray<D>> for WKBArray<O> {
    fn from(value: &PointArray<D>) -> Self {
        let non_null_count = value
            .nulls()
            .map_or(value.len(), |validity| value.len() - validity.null_count());

        let validity = value.nulls().cloned();
        // only allocate space for a WKBPoint for non-null items
        let values_len = non_null_count * point_wkb_size_const::<D>();
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

        let values = {
            let values = Vec::with_capacity(values_len);
            let mut writer = Cursor::new(values);

            for maybe_geom in value.iter() {
                if let Some(geom) = maybe_geom {
                    write_point_as_wkb(&mut writer, &geom).unwrap();
                    offsets.try_push_usize(point_wkb_size_const::<D>()).unwrap();
                } else {
                    offsets.extend_constant(1);
                }
            }

            writer.into_inner()
        };

        let binary_arr = GenericBinaryArray::new(offsets.into(), values.into(), validity);
        WKBArray::new(binary_arr, value.metadata())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::{p0, p1, p2};

    #[test]
    fn round_trip() {
        // TODO: test with nulls
        let orig_arr: PointArray<2> = vec![Some(p0()), Some(p1()), Some(p2())].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: PointArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }

    #[test]
    fn round_trip_with_null() {
        let orig_arr: PointArray<2> = vec![Some(p0()), None, Some(p1()), None, Some(p2())].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: PointArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
