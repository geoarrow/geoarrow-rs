use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::GeometryCollectionArray;
use crate::scalar::Geometry;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use crate::{ArrayBase, NativeArray};
use geo_traits::to_geo::ToGeoGeometryCollection;
use geo_traits::GeometryCollectionTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a GeometryCollection
#[derive(Debug, Clone)]
pub struct GeometryCollection {
    array: GeometryCollectionArray,
    start_offset: usize,
}

impl GeometryCollection {
    pub fn new(array: GeometryCollectionArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        let (start_offset, _) = array.geom_offsets.start_end(0);
        Self {
            array,
            start_offset,
        }
    }

    pub fn into_inner(self) -> GeometryCollectionArray {
        self.array
    }
}

impl NativeScalar for GeometryCollection {
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

impl GeometryCollectionTrait for GeometryCollection {
    type T = f64;
    type GeometryType<'b>
        = Geometry
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.dimension().into()
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        self.array.array.value(self.start_offset + i)
    }
}

impl GeometryCollectionTrait for &GeometryCollection {
    type T = f64;
    type GeometryType<'b>
        = Geometry
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.dimension().into()
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        self.array.array.value(self.start_offset + i)
    }
}

impl From<&GeometryCollection> for geo::GeometryCollection {
    fn from(value: &GeometryCollection) -> Self {
        value.to_geometry_collection()
    }
}

impl From<GeometryCollection> for geo::Geometry {
    fn from(value: GeometryCollection) -> Self {
        geo::Geometry::GeometryCollection(value.into())
    }
}

impl RTreeObject for GeometryCollection {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        todo!()
        // let (lower, upper) = bounding_rect_multilinestring(self);
        // AABB::from_corners(lower, upper)
    }
}

impl<G: GeometryCollectionTrait<T = f64>> PartialEq<G> for GeometryCollection {
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}
