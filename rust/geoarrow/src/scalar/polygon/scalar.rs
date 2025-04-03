use crate::algorithm::native::bounding_rect::bounding_rect_polygon;
use crate::algorithm::native::eq::polygon_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::CoordBuffer;
use geoarrow_schema::Dimension;
use crate::scalar::LineString;
use crate::trait_::NativeScalar;
use arrow_buffer::OffsetBuffer;
use geo_traits::to_geo::ToGeoPolygon;
use geo_traits::PolygonTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Polygon
///
/// This implements [PolygonTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct Polygon<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> Polygon<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i32>,
        ring_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
        }
    }

    pub(crate) fn into_owned_inner(
        self,
    ) -> (CoordBuffer, OffsetBuffer<i32>, OffsetBuffer<i32>, usize) {
        (
            self.coords.clone(),
            self.geom_offsets.clone(),
            self.ring_offsets.clone(),
            self.geom_index,
        )
    }
}

impl NativeScalar for Polygon<'_> {
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

impl<'a> PolygonTrait for Polygon<'a> {
    type T = f64;
    type RingType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(self.coords, self.ring_offsets, start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + 1 + i)
    }
}

impl<'a> PolygonTrait for &'a Polygon<'a> {
    type T = f64;
    type RingType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(self.coords, self.ring_offsets, start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + 1 + i)
    }
}

impl From<Polygon<'_>> for geo::Polygon {
    fn from(value: Polygon<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Polygon<'_>> for geo::Polygon {
    fn from(value: &Polygon<'_>) -> Self {
        value.to_polygon()
    }
}

impl From<Polygon<'_>> for geo::Geometry {
    fn from(value: Polygon<'_>) -> Self {
        geo::Geometry::Polygon(value.into())
    }
}

impl RTreeObject for Polygon<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_polygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: PolygonTrait<T = f64>> PartialEq<G> for Polygon<'_> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::PolygonArray;
    use geoarrow_schema::Dimension;
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
