use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{MultiLineStringArray, WKBArray};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use std::io::Cursor;
use wkb::writer::{multi_line_string_wkb_size, write_multi_line_string};
use wkb::Endianness;

impl<O: OffsetSizeTrait> From<&MultiLineStringArray> for WKBArray<O> {
    fn from(value: &MultiLineStringArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

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
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_multi_line_string(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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
    use crate::test::multilinestring::{ml0, ml1};
    use geoarrow_schema::Dimension;

    #[test]
    fn round_trip() {
        let orig_arr: MultiLineStringArray =
            (vec![Some(ml0()), Some(ml1()), None], Dimension::XY).into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiLineStringArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
