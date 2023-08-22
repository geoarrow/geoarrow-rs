use crate::array::{GeometryCollectionArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::reader::geometry::Endianness;
use crate::io::wkb::writer::geometry::{geometry_wkb_size, write_geometry_as_wkb};
use crate::trait_::GeometryArrayTrait;
use arrow2::array::BinaryArray;
use arrow2::datatypes::DataType;
use arrow2::offset::Offsets;
use arrow2::types::Offset;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBGeometryCollection
pub fn geometry_collection_wkb_size<'a>(geom: &'a impl GeometryCollectionTrait<'a>) -> usize {
    let mut sum = 1 + 4 + 4;

    for geom_idx in 0..geom.num_geometries() {
        let inner_geom = geom.geometry(geom_idx).unwrap();
        sum += geometry_wkb_size(&inner_geom);
    }

    sum
}

/// Write a GeometryCollection geometry to a Writer encoded as WKB
pub fn write_geometry_collection_as_wkb<'a, W: Write>(
    mut writer: W,
    geom: &'a impl GeometryCollectionTrait<'a, T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 7
    writer.write_u32::<LittleEndian>(7).unwrap();

    // numGeometries
    writer
        .write_u32::<LittleEndian>(geom.num_geometries().try_into().unwrap())
        .unwrap();

    for geom_idx in 0..geom.num_geometries() {
        let inner_geom = geom.geometry(geom_idx).unwrap();
        write_geometry_as_wkb(&mut writer, &inner_geom).unwrap();
    }

    Ok(())
}

impl<A: Offset, B: Offset> From<&GeometryCollectionArray<A>> for WKBArray<B> {
    fn from(value: &GeometryCollectionArray<A>) -> Self {
        let mut offsets: Offsets<B> = Offsets::with_capacity(value.len());

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
            let values = Vec::with_capacity(offsets.last().to_usize());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_geometry_collection_as_wkb(&mut writer, &geom).unwrap();
            }

            writer.into_inner()
        };

        let data_type = match B::IS_LARGE {
            true => DataType::LargeBinary,
            false => DataType::Binary,
        };

        let binary_arr = BinaryArray::new(
            data_type,
            offsets.into(),
            values.into(),
            value.validity().cloned(),
        );
        WKBArray::new(binary_arr)
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::multipolygon::{mp0, mp1};

//     #[test]
//     fn round_trip() {
//         let orig_arr: GeometryCollectionArray<i32> = vec![Some(mp0()), Some(mp1()), None].into();
//         let wkb_arr: WKBArray<i32> = (&orig_arr).into();
//         let new_arr: GeometryCollectionArray<i32> = wkb_arr.try_into().unwrap();

//         assert_eq!(orig_arr, new_arr);
//     }
// }
