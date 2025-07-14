use crate::ArrayBase;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{RectArray, WKBArray};
use crate::trait_::ArrayAccessor;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use std::io::Cursor;
use wkb::Endianness;
use wkb::writer::{rect_wkb_size, write_rect};

impl<O: OffsetSizeTrait> From<&RectArray> for WKBArray<O> {
    fn from(value: &RectArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(rect_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_rect(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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
