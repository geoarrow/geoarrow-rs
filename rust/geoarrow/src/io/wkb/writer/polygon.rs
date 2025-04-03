use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{PolygonArray, WKBArray};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use std::io::Cursor;
use wkb::writer::{polygon_wkb_size, write_polygon};
use wkb::Endianness;

impl<O: OffsetSizeTrait> From<&PolygonArray> for WKBArray<O> {
    fn from(value: &PolygonArray) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets.try_push_usize(polygon_wkb_size(&geom)).unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_polygon(&mut writer, &geom, Endianness::LittleEndian).unwrap();
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
    use geoarrow_schema::Dimension;
    use crate::test::polygon::{p0, p1};
    use crate::trait_::ArrayAccessor;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn round_trip() {
        let orig_arr: PolygonArray = (vec![Some(p0()), Some(p1()), None], Dimension::XY).into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: PolygonArray = (wkb_arr.clone(), Dimension::XY).try_into().unwrap();

        let wkb0 = geo::Geometry::Polygon(p0())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb1 = geo::Geometry::Polygon(p1())
            .to_wkb(CoordDimensions::xy())
            .unwrap();

        assert_eq!(wkb_arr.value(0).as_ref(), &wkb0);
        assert_eq!(wkb_arr.value(1).as_ref(), &wkb1);

        assert_eq!(orig_arr, new_arr);
    }
}
