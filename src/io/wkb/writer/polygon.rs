use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{PolygonArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use crate::io::wkb::reader::geometry::Endianness;
use crate::trait_::GeometryArrayTrait;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBPolygon
pub fn polygon_wkb_size(geom: &impl PolygonTrait) -> usize {
    let mut sum = 1 + 4 + 4;

    // TODO: support empty polygons where this will panic
    let ext_ring = geom.exterior().unwrap();
    sum += 4 + (ext_ring.num_coords() * 16);

    for int_ring_idx in 0..geom.num_interiors() {
        let int_ring = geom.interior(int_ring_idx).unwrap();
        sum += 4 + (int_ring.num_coords() * 16);
    }

    sum
}

/// Write a Polygon geometry to a Writer encoded as WKB
pub fn write_polygon_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl PolygonTrait<T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 3
    writer.write_u32::<LittleEndian>(3).unwrap();

    // numRings
    // TODO: support empty polygons where this will panic
    let num_rings = 1 + geom.num_interiors();
    writer
        .write_u32::<LittleEndian>(num_rings.try_into().unwrap())
        .unwrap();

    let ext_ring = geom.exterior().unwrap();
    writer
        .write_u32::<LittleEndian>(ext_ring.num_coords().try_into().unwrap())
        .unwrap();

    for coord_idx in 0..ext_ring.num_coords() {
        let coord = ext_ring.coord(coord_idx).unwrap();
        writer.write_f64::<LittleEndian>(coord.x()).unwrap();
        writer.write_f64::<LittleEndian>(coord.y()).unwrap();
    }

    for int_ring_idx in 0..geom.num_interiors() {
        let int_ring = geom.interior(int_ring_idx).unwrap();
        writer
            .write_u32::<LittleEndian>(int_ring.num_coords().try_into().unwrap())
            .unwrap();

        for coord_idx in 0..int_ring.num_coords() {
            let coord = int_ring.coord(coord_idx).unwrap();
            writer.write_f64::<LittleEndian>(coord.x()).unwrap();
            writer.write_f64::<LittleEndian>(coord.y()).unwrap();
        }
    }

    Ok(())
}

impl<A: OffsetSizeTrait, B: OffsetSizeTrait> From<&PolygonArray<A>> for WKBArray<B> {
    fn from(value: &PolygonArray<A>) -> Self {
        let mut offsets: OffsetsBuilder<B> = OffsetsBuilder::with_capacity(value.len());

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
                write_polygon_as_wkb(&mut writer, &geom).unwrap();
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
    use crate::test::polygon::{p0, p1};
    use crate::trait_::GeometryArrayAccessor;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn round_trip() {
        let orig_arr: PolygonArray<i32> = vec![Some(p0()), Some(p1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: PolygonArray<i32> = wkb_arr.clone().try_into().unwrap();

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

    // // TODO: parsing WKBArray<i64> into LineStringArray<i32> not yet implemented
    // fn round_trip_to_i64() {
    //     let orig_arr: LineStringArray<i32> = vec![Some(ls0()), Some(ls1()), None].into();
    //     let wkb_arr: WKBArray<i64> = (&orig_arr).into();
    //     let new_arr: LineStringArray<i32> = wkb_arr.try_into().unwrap();

    //     assert_eq!(orig_arr, new_arr);
    // }
}
