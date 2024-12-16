use crate::algorithm::native::bounding_rect::bounding_rect_multipolygon;
use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{MultiPolygonArray, PolygonArray};
use crate::scalar::Polygon;
use crate::trait_::{ArrayAccessor, NativeScalar};
use crate::ArrayBase;
use geo_traits::to_geo::ToGeoMultiPolygon;
use geo_traits::MultiPolygonTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPolygon
#[derive(Debug, Clone)]
pub struct MultiPolygon {
    array: MultiPolygonArray,
    start_offset: usize,
}

impl MultiPolygon {
    pub fn new(array: MultiPolygonArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        let (start_offset, _) = array.geom_offsets.start_end(0);
        Self {
            array,
            start_offset,
        }
    }

    pub fn into_inner(self) -> MultiPolygonArray {
        self.array
    }
}

impl NativeScalar for MultiPolygon {
    type ScalarGeo = geo::MultiPolygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::MultiPolygon(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a> MultiPolygonTrait for MultiPolygon {
    type T = f64;
    type PolygonType<'b>
        = Polygon
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        let arr = PolygonArray::new(
            self.array.coords.clone(),
            self.array.polygon_offsets.clone(),
            self.array.ring_offsets.clone(),
            None,
            Default::default(),
        );
        arr.value(self.start_offset + i)
    }
}

impl<'a> MultiPolygonTrait for &'a MultiPolygon {
    type T = f64;
    type PolygonType<'b>
        = Polygon
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        let arr = PolygonArray::new(
            self.array.coords.clone(),
            self.array.polygon_offsets.clone(),
            self.array.ring_offsets.clone(),
            None,
            Default::default(),
        );
        arr.value(self.start_offset + i)
    }
}

impl From<MultiPolygon> for geo::MultiPolygon {
    fn from(value: MultiPolygon) -> Self {
        (&value).into()
    }
}

impl From<&MultiPolygon> for geo::MultiPolygon {
    fn from(value: &MultiPolygon) -> Self {
        value.to_multi_polygon()
    }
}

impl From<MultiPolygon> for geo::Geometry {
    fn from(value: MultiPolygon) -> Self {
        geo::Geometry::MultiPolygon(value.into())
    }
}

impl RTreeObject for MultiPolygon {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipolygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: MultiPolygonTrait<T = f64>> PartialEq<G> for MultiPolygon {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPolygonArray;
    use crate::datatypes::Dimension;
    use crate::test::multipolygon::{mp0, mp1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPolygonArray = (vec![mp0(), mp1()].as_slice(), Dimension::XY).into();
        let arr2: MultiPolygonArray = (vec![mp0(), mp0()].as_slice(), Dimension::XY).into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
