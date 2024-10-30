use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{PolygonArray, WKBArray};
use crate::error::WKBResult;
use crate::common::WKBType;
use crate::reader::Endianness;
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use byteorder::{LittleEndian, WriteBytesExt};
use geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
use std::io::{Cursor, Write};

/// The byte length of a WKBPolygon
pub fn polygon_wkb_size(geom: &impl PolygonTrait) -> usize {
    let mut sum = 1 + 4 + 4;

    let each_coord = geom.dim().size() * 8;

    // TODO: support empty polygons where this will panic
    let ext_ring = geom.exterior().unwrap();
    sum += 4 + (ext_ring.num_coords() * each_coord);

    for int_ring in geom.interiors() {
        sum += 4 + (int_ring.num_coords() * each_coord);
    }

    sum
}

/// Write a Polygon geometry to a Writer encoded as WKB
pub fn write_polygon_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl PolygonTrait<T = f64>,
) -> WKBResult<()> {
    use geo_traits::Dimensions;

    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    match geom.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => {
            writer
                .write_u32::<LittleEndian>(WKBType::Polygon.into())
                .unwrap();
        }
        Dimensions::Xyz | Dimensions::Unknown(3) => {
            writer
                .write_u32::<LittleEndian>(WKBType::PolygonZ.into())
                .unwrap();
        }
        _ => panic!(),
    }

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

    for coord in ext_ring.coords() {
        writer.write_f64::<LittleEndian>(coord.x()).unwrap();
        writer.write_f64::<LittleEndian>(coord.y()).unwrap();
        if geom.dim().size() == 3 {
            writer
                .write_f64::<LittleEndian>(coord.nth_unchecked(2))
                .unwrap();
        }
    }

    for int_ring in geom.interiors() {
        writer
            .write_u32::<LittleEndian>(int_ring.num_coords().try_into().unwrap())
            .unwrap();

        for coord in int_ring.coords() {
            writer.write_f64::<LittleEndian>(coord.x()).unwrap();
            writer.write_f64::<LittleEndian>(coord.y()).unwrap();
            if geom.dim().size() == 3 {
                writer
                    .write_f64::<LittleEndian>(coord.nth_unchecked(2))
                    .unwrap();
            }
        }
    }

    Ok(())
}

impl<O: OffsetSizeTrait, const D: usize> From<&PolygonArray<D>> for WKBArray<O> {
    fn from(value: &PolygonArray<D>) -> Self {
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
                write_polygon_as_wkb(&mut writer, &geom).unwrap();
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
    use crate::test::polygon::{p0, p1};
    use crate::trait_::ArrayAccessor;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn round_trip() {
        let orig_arr: PolygonArray<2> = vec![Some(p0()), Some(p1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: PolygonArray<2> = wkb_arr.clone().try_into().unwrap();

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
