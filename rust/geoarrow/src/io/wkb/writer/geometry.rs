use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use wkb::writer::{geometry_wkb_size, write_geometry};
use wkb::Endianness;

use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{GeometryArray, MixedGeometryArray, WKBArray};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use std::io::Cursor;

impl<O: OffsetSizeTrait> From<&MixedGeometryArray> for WKBArray<O> {
    fn from(value: &MixedGeometryArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(geometry_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_geometry(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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

impl<O: OffsetSizeTrait> From<&GeometryArray> for WKBArray<O> {
    fn from(value: &GeometryArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(geometry_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_geometry(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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
    use crate::test::multilinestring::{ml0, ml1};
    use crate::test::point::{p0, p1};

    #[test]
    fn round_trip() {
        let orig_arr: MixedGeometryArray = (
            vec![
                Some(geo::Geometry::MultiLineString(ml0())),
                Some(geo::Geometry::MultiLineString(ml1())),
                Some(geo::Geometry::Point(p0())),
                Some(geo::Geometry::Point(p1())),
            ],
            Dimension::XY,
        )
            .try_into()
            .unwrap();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MixedGeometryArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }

    #[ignore = "None not allowed in geometry array."]
    #[test]
    fn round_trip_null() {
        let orig_arr: MixedGeometryArray = (
            vec![
                Some(geo::Geometry::MultiLineString(ml0())),
                Some(geo::Geometry::MultiLineString(ml1())),
                Some(geo::Geometry::Point(p0())),
                Some(geo::Geometry::Point(p1())),
                None,
            ],
            Dimension::XY,
        )
            .try_into()
            .unwrap();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MixedGeometryArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
