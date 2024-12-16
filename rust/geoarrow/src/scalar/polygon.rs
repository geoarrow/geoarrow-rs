use crate::algorithm::native::bounding_rect::bounding_rect_polygon;
use crate::algorithm::native::eq::polygon_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{LineStringArray, PolygonArray};
use crate::scalar::LineString;
use crate::trait_::{ArrayAccessor, NativeScalar};
use crate::ArrayBase;
use geo_traits::to_geo::ToGeoPolygon;
use geo_traits::PolygonTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Polygon
///
/// This is stored as a [PolygonArray] with length 1. That element may not be null.
#[derive(Debug, Clone)]
pub struct Polygon {
    array: PolygonArray,
    start_offset: usize,
}

// <'a> {
//     pub(crate) coords: &'a CoordBuffer,

//     /// Offsets into the ring array where each geometry starts
//     pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

//     /// Offsets into the coordinate array where each ring starts
//     pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

//     pub(crate) geom_index: usize,

//     start_offset: usize,
// }

impl Polygon {
    pub fn new(array: PolygonArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        let (start_offset, _) = array.geom_offsets.start_end(0);
        Self {
            array,
            start_offset,
        }
    }

    pub fn into_inner(self) -> PolygonArray {
        self.array
    }
}

impl NativeScalar for Polygon {
    type ScalarGeo = geo::Polygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::Polygon(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl PolygonTrait for Polygon {
    type T = f64;
    type RingType<'b>
        = LineString
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.array.geom_offsets.start_end(0);
        if start == end {
            None
        } else {
            let arr = LineStringArray::new(
                self.array.coords.clone(),
                self.array.ring_offsets.clone(),
                None,
                Default::default(),
            );
            Some(arr.value(start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        let arr = LineStringArray::new(
            self.array.coords.clone(),
            self.array.ring_offsets.clone(),
            None,
            Default::default(),
        );
        arr.value(self.start_offset + 1 + i)
    }
}

impl PolygonTrait for &Polygon {
    type T = f64;
    type RingType<'b>
        = LineString
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.array.geom_offsets.start_end(0);
        if start == end {
            None
        } else {
            let arr = LineStringArray::new(
                self.array.coords.clone(),
                self.array.ring_offsets.clone(),
                None,
                Default::default(),
            );
            Some(arr.value(start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        let arr = LineStringArray::new(
            self.array.coords.clone(),
            self.array.ring_offsets.clone(),
            None,
            Default::default(),
        );
        arr.value(self.start_offset + 1 + i)
    }
}

impl From<Polygon> for geo::Polygon {
    fn from(value: Polygon) -> Self {
        (&value).into()
    }
}

impl From<&Polygon> for geo::Polygon {
    fn from(value: &Polygon) -> Self {
        value.to_polygon()
    }
}

impl From<Polygon> for geo::Geometry {
    fn from(value: Polygon) -> Self {
        geo::Geometry::Polygon(value.into())
    }
}

impl RTreeObject for Polygon {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_polygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: PolygonTrait<T = f64>> PartialEq<G> for Polygon {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::PolygonArray;
    use crate::datatypes::Dimension;
    use crate::test::polygon::{p0, p1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: PolygonArray = (vec![p0(), p1()].as_slice(), Dimension::XY).into();
        let arr2: PolygonArray = (vec![p0(), p0()].as_slice(), Dimension::XY).into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
