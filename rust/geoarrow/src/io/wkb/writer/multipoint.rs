use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{MultiPointArray, WKBArray};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use std::io::Cursor;
use wkb::writer::{multi_point_wkb_size, write_multi_point};
use wkb::Endianness;

impl<O: OffsetSizeTrait> From<&MultiPointArray> for WKBArray<O> {
    fn from(value: &MultiPointArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

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
                write_multi_point(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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
    use crate::datatypes::Dimension;
    use crate::test::multipoint::{mp0, mp1};

    #[test]
    fn round_trip() {
        let orig_arr: MultiPointArray =
            (vec![Some(mp0()), Some(mp1()), None], Dimension::XY).into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiPointArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
