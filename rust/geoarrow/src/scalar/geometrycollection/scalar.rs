use crate::NativeArray;
use crate::algorithm::native::bounding_rect::bounding_rect_geometry_collection;
use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::MixedGeometryArray;
use crate::array::util::OffsetBufferUtils;
use crate::io::geo::geometry_collection_to_geo;
use crate::scalar::Geometry;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use arrow_buffer::OffsetBuffer;
use geo_traits::GeometryCollectionTrait;
use geoarrow_schema::Dimension;
use rstar::{AABB, RTreeObject};

/// An Arrow equivalent of a GeometryCollection
///
/// This implements [GeometryCollectionTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct GeometryCollection<'a> {
    pub(crate) array: &'a MixedGeometryArray,

    /// Offsets into the geometry array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> GeometryCollection<'a> {
    pub(crate) fn new(
        array: &'a MixedGeometryArray,
        geom_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            array,
            geom_offsets,
            geom_index,
            start_offset,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn into_inner(&self) -> (&MixedGeometryArray, &OffsetBuffer<i32>, usize) {
        (self.array, self.geom_offsets, self.geom_index)
    }
}

impl NativeScalar for GeometryCollection<'_> {
    type ScalarGeo = geo::GeometryCollection;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::GeometryCollection(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a> GeometryCollectionTrait for GeometryCollection<'a> {
    type T = f64;
    type GeometryType<'b>
        = Geometry<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.array.dimension() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            _ => todo!("XYM and XYZM not supported yet"),
        }
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        self.array.value(self.start_offset + i)
    }
}

impl<'a> GeometryCollectionTrait for &'a GeometryCollection<'a> {
    type T = f64;
    type GeometryType<'b>
        = Geometry<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.array.dimension() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            _ => todo!("XYM and XYZM not supported yet"),
        }
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        self.array.value(self.start_offset + i)
    }
}

impl From<&GeometryCollection<'_>> for geo::GeometryCollection {
    fn from(value: &GeometryCollection<'_>) -> Self {
        geometry_collection_to_geo(value)
    }
}

impl From<GeometryCollection<'_>> for geo::GeometryCollection {
    fn from(value: GeometryCollection<'_>) -> Self {
        (&value).into()
    }
}

// We can't implement both
// ```
// impl From<GeometryCollection<'_>> for geo::GeometryCollection
// ```
// and
// ```
// impl From<GeometryCollection<'_>> for geo::Geometry
// ```
// because of this problematic blanket impl
// (https://github.com/georust/geo/blob/ef55eabe9029b27f753d4c40db9f656e3670202e/geo-types/src/geometry/geometry_collection.rs#L113-L120).
//
// Thus we need to choose either one or the other to implement.
//
// If we implemented only `for geo::Geometry`, then the default blanket impl for
// `geo::GeometryCollection` would be **wrong** because it would doubly-nest the
// `GeometryCollection`:
//
// ```rs
// GeometryCollection(
//     [
//         GeometryCollection(
//             GeometryCollection(
//                 [
//                     Point(
//                         Point(
//                             Coord {
//                                 x: 0.0,
//                                 y: 0.0,
//                             },
//                         ),
//                     ),
//                 ],
//             ),
//         ),
//     ],
// )
// ```
//
// Therefore we must implement only `for geo::GeometryCollection`
// impl From<&GeometryCollection<'_>> for geo::Geometry {
//     fn from(value: &GeometryCollection<'_>) -> Self {
//         geo::Geometry::GeometryCollection(geometry_collection_to_geo(value))
//     }
// }

// impl From<GeometryCollection<'_>> for geo::Geometry {
//     fn from(value: GeometryCollection<'_>) -> Self {
//         geo::Geometry::GeometryCollection(geometry_collection_to_geo(&value))
//     }
// }

impl RTreeObject for GeometryCollection<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_geometry_collection(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: GeometryCollectionTrait<T = f64>> PartialEq<G> for GeometryCollection<'_> {
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::OffsetBufferBuilder;

    use crate::array::PointArray;

    use super::*;

    #[test]
    fn stack_overflow_repro_issue_979() {
        let orig_point = geo::point!(x: 0., y: 0.);
        let array: MixedGeometryArray =
            PointArray::from((vec![orig_point].as_slice(), Dimension::XY)).into();
        let mut offsets = OffsetBufferBuilder::new(1);
        offsets.push_length(1);
        let offsets = offsets.finish();
        let gc = GeometryCollection::new(&array, &offsets, 0);

        let out: geo::GeometryCollection = gc.into();
        assert_eq!(out.0.len(), 1, "should be one point");
        assert_eq!(out.0[0], geo::Geometry::Point(orig_point));
    }
}
