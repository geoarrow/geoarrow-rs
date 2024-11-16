use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::MixedGeometryArray;
use crate::datatypes::Dimension;
use crate::io::geo::geometry_collection_to_geo;
use crate::scalar::Geometry;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_buffer::OffsetBuffer;
use geo_traits::GeometryCollectionTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a GeometryCollection
#[derive(Debug, Clone)]
pub struct GeometryCollection<'a> {
    pub(crate) array: &'a MixedGeometryArray,

    /// Offsets into the geometry array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> GeometryCollection<'a> {
    pub fn new(
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

    pub fn into_inner(&self) -> (&MixedGeometryArray, &OffsetBuffer<i32>, usize) {
        (self.array, self.geom_offsets, self.geom_index)
    }
}

impl<'a> NativeScalar for GeometryCollection<'a> {
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
    type GeometryType<'b> = Geometry<'a> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.array.dimension() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
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
    type GeometryType<'b> = Geometry<'a> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.array.dimension() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
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

impl From<GeometryCollection<'_>> for geo::Geometry {
    fn from(value: GeometryCollection<'_>) -> Self {
        geo::Geometry::GeometryCollection(value.into())
    }
}

impl RTreeObject for GeometryCollection<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        todo!()
        // let (lower, upper) = bounding_rect_multilinestring(self);
        // AABB::from_corners(lower, upper)
    }
}

impl<G: GeometryCollectionTrait<T = f64>> PartialEq<G> for GeometryCollection<'_> {
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}
