use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{MultiPolygonArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::MultiPolygonTrait;
use crate::io::wkb::common::WKBType;
use crate::io::wkb::reader::Endianness;
use crate::io::wkb::writer::polygon::{polygon_wkb_size, write_polygon_as_wkb};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBMultiPolygon
pub fn multi_polygon_wkb_size(geom: &impl MultiPolygonTrait) -> usize {
    let mut sum = 1 + 4 + 4;
    for polygon in geom.polygons() {
        sum += polygon_wkb_size(&polygon);
    }

    sum
}

/// Write a MultiPolygon geometry to a Writer encoded as WKB
pub fn write_multi_polygon_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl MultiPolygonTrait<T = f64>,
) -> Result<()> {
    use crate::geo_traits::Dimensions;

    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    match geom.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => {
            writer
                .write_u32::<LittleEndian>(WKBType::MultiPolygon.into())
                .unwrap();
        }
        Dimensions::Xyz | Dimensions::Unknown(3) => {
            writer
                .write_u32::<LittleEndian>(WKBType::MultiPolygonZ.into())
                .unwrap();
        }
        _ => panic!(),
    }

    // numPolygons
    writer
        .write_u32::<LittleEndian>(geom.num_polygons().try_into().unwrap())
        .unwrap();

    for polygon in geom.polygons() {
        write_polygon_as_wkb(&mut writer, &polygon).unwrap();
    }

    Ok(())
}

impl<O: OffsetSizeTrait, const D: usize> From<&MultiPolygonArray<D>> for WKBArray<O> {
    fn from(value: &MultiPolygonArray<D>) -> Self {
        let mut offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(value.len());

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
    use crate::test::multipolygon::{mp0, mp1};

    #[test]
    fn round_trip() {
        let orig_arr: MultiPolygonArray<2> = vec![Some(mp0()), Some(mp1()), None].into();
        let wkb_arr: WKBArray<i32> = (&orig_arr).into();
        let new_arr: MultiPolygonArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(orig_arr, new_arr);
    }
}
