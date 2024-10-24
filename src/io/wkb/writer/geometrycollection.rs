use crate::array::offset_builder::OffsetsBuilder;
use crate::array::{GeometryCollectionArray, WKBArray};
use crate::error::Result;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::wkb::common::WKBType;
use crate::io::wkb::reader::Endianness;
use crate::io::wkb::writer::geometry::{geometry_wkb_size, write_geometry_as_wkb};
use crate::trait_::ArrayAccessor;
use crate::ArrayBase;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use arrow_buffer::Buffer;
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
    use crate::geo_traits::Dimensions;

    // Byte order
    writer.write_u8(Endianness::LittleEndian.into())?;

    match geom.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => {
            writer.write_u32::<LittleEndian>(WKBType::GeometryCollection.into())?;
        }
        Dimensions::Xyz | Dimensions::Unknown(3) => {
            writer.write_u32::<LittleEndian>(WKBType::GeometryCollectionZ.into())?;
        }
        _ => panic!(),
    }

    // numGeometries
    writer.write_u32::<LittleEndian>(geom.num_geometries().try_into().unwrap())?;

    for inner_geom in geom.geometries() {
        write_geometry_as_wkb(&mut writer, &inner_geom)?;
    }

    Ok(())
}

impl<O: OffsetSizeTrait, const D: usize> From<&GeometryCollectionArray<D>> for WKBArray<O> {
    fn from(value: &GeometryCollectionArray<D>) -> Self {
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
                write_geometry_collection_as_wkb(&mut writer, &geom).unwrap();
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
