use arrow_buffer::OffsetBuffer;
use geo_traits::GeometryCollectionTrait;
use geoarrow_schema::Dimension;

use crate::algorithm::native::bounding_rect::bounding_rect_geometry_collection;
use crate::array::MixedGeometryArray;
use crate::eq::geometry_collection_eq;
use crate::scalar::Geometry;
use crate::trait_::ArrayAccessor;
use crate::util::OffsetBufferUtils;
use crate::NativeArray;

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
