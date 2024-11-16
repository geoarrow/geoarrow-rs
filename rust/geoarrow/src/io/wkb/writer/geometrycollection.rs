use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{GeometryCollectionArray, WKBArray};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use std::io::Cursor;
use wkb::writer::{geometry_collection_wkb_size, write_geometry_collection};
use wkb::Endianness;

impl<O: OffsetSizeTrait> From<&GeometryCollectionArray> for WKBArray<O> {
    fn from(value: &GeometryCollectionArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets
                    .try_push_usize(geometry_collection_wkb_size(&geom))
                    .unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().as_usize());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_geometry_collection(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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
    use crate::test::multipoint;
    use crate::test::multipolygon;

    #[ignore = "failing test, need to come back to this"]
    #[test]
    fn round_trip() {
        let gc0 = geo::GeometryCollection::new_from(vec![
            geo::Geometry::MultiPoint(multipoint::mp0()),
            geo::Geometry::MultiPolygon(multipolygon::mp0()),
        ]);
        let gc1 = geo::GeometryCollection::new_from(vec![
            geo::Geometry::MultiPoint(multipoint::mp1()),
            geo::Geometry::MultiPolygon(multipolygon::mp1()),
        ]);

        let orig_arr: GeometryCollectionArray =
            (vec![Some(gc0), Some(gc1), None], Dimension::XY).into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: GeometryCollectionArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
