use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{GeometryCollectionArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::reader::Endianness;
use crate::io::wkb::writer::geometry::{geometry_wkb_size, write_geometry_as_wkb};
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::GeometryArrayTrait;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

/// The byte length of a WKBGeometryCollection
pub fn geometry_collection_wkb_size(geom: &impl GeometryCollectionTrait) -> usize {
    let mut sum = 1 + 4 + 4;

    for inner_geom in geom.geometries() {
        sum += geometry_wkb_size(&inner_geom);
    }

    sum
}

/// Write a GeometryCollection geometry to a Writer encoded as WKB
pub fn write_geometry_collection_as_wkb<W: Write>(
    mut writer: W,
    geom: &impl GeometryCollectionTrait<T = f64>,
) -> Result<()> {
    // Byte order
    writer.write_u8(Endianness::LittleEndian.into()).unwrap();

    // wkbType = 7
    writer.write_u32::<LittleEndian>(7).unwrap();

    // numGeometries
    writer
        .write_u32::<LittleEndian>(geom.num_geometries().try_into().unwrap())
        .unwrap();

    for inner_geom in geom.geometries() {
        write_geometry_as_wkb(&mut writer, &inner_geom).unwrap();
    }

    Ok(())
}

impl<A: OffsetSizeTrait, B: OffsetSizeTrait> From<&GeometryCollectionArray<A>> for WKBArray<B> {
    fn from(value: &GeometryCollectionArray<A>) -> Self {
        let mut offsets: OffsetsBuilder<B> = OffsetsBuilder::with_capacity(value.len());

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
            let values = Vec::with_capacity(offsets.last().to_usize().unwrap());
            let mut writer = Cursor::new(values);

            for geom in value.iter().flatten() {
                write_geometry_collection_as_wkb(&mut writer, &geom).unwrap();
            }

            writer.into_inner()
        };

        let binary_arr =
            GenericBinaryArray::new(offsets.into(), values.into(), value.nulls().cloned());
        WKBArray::new(binary_arr, value.metadata())
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::multipoint;
//     use crate::test::multipolygon;

//     #[test]
//     fn round_trip() {
//         let gc0 = geo::GeometryCollection::new_from(vec![
//             geo::Geometry::MultiPoint(multipoint::mp0()),
//             geo::Geometry::MultiPolygon(multipolygon::mp0()),
//         ]);
//         let gc1 = geo::GeometryCollection::new_from(vec![
//             geo::Geometry::MultiPoint(multipoint::mp1()),
//             geo::Geometry::MultiPolygon(multipolygon::mp1()),
//         ]);

//         let orig_arr: GeometryCollectionArray<i32> = vec![Some(gc0), Some(gc1), None].into();
//         let wkb_arr: WKBArray<i32> = (&orig_arr).into();
//         let new_arr: GeometryCollectionArray<i32> = wkb_arr.try_into().unwrap();

//         assert_eq!(orig_arr, new_arr);
//     }
// }
