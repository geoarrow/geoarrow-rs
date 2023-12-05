use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{MultiPolygonArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::MultiPolygonTrait;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::writer::polygon::{polygon_wkb_size, write_polygon_as_wkb};
use crate::trait_::GeometryArrayTrait;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBMultiPolygon
pub fn multi_polygon_wkb_size(geom: &impl MultiPolygonTrait) -> usize {
    let mut sum = 1 + 4 + 4;
    for polygon_idx in 0..geom.num_polygons() {
        let polygon = geom.polygon(polygon_idx).unwrap();
        sum += polygon_wkb_size(&polygon);
    }

    sum
}

/// Write a MultiPolygon geometry to a Writer encoded as WKB
pub fn write_multi_polygon_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl MultiPolygonTrait<T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 6
    writer.write_u32::<LittleEndian>(6).unwrap();

    // numPolygons
    writer
        .write_u32::<LittleEndian>(geom.num_polygons().try_into().unwrap())
        .unwrap();

    for polygon_idx in 0..geom.num_polygons() {
        let polygon = geom.polygon(polygon_idx).unwrap();
        write_polygon_as_wkb(&mut writer, &polygon).unwrap();
    }

    Ok(())
}

impl<A: OffsetSizeTrait, B: OffsetSizeTrait> From<&MultiPolygonArray<A>> for WKBArray<B> {
    fn from(value: &MultiPolygonArray<A>) -> Self {
        let mut offsets: OffsetsBuilder<B> = OffsetsBuilder::with_capacity(value.len());

        // First pass: calculate binary array offsets
        for maybe_geom in value.iter() {
            if let Some(geom) = maybe_geom {
                offsets
                    .try_push_usize(multi_polygon_wkb_size(&geom))
                    .unwrap();
            } else {
                offsets.extend_constant(1);
            }
        }

        let values = {
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_multi_polygon_as_wkb(&mut writer, &geom).unwrap();
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
    use crate::test::multipolygon::{mp0, mp1};

    #[test]
    fn round_trip() {
        let orig_arr: MultiPolygonArray<i32> = vec![Some(mp0()), Some(mp1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiPolygonArray<i32> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
