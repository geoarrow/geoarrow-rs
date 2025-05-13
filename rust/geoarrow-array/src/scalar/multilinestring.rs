use arrow_buffer::OffsetBuffer;
use geo_traits::MultiLineStringTrait;

use crate::array::CoordBuffer;
use crate::eq::multi_line_string_eq;
use crate::scalar::LineString;
use crate::util::OffsetBufferUtils;

/// An Arrow equivalent of a MultiLineString
///
/// This implements [MultiLineStringTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct MultiLineString<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> MultiLineString<'a> {
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
}

impl<'a> MultiLineStringTrait for MultiLineString<'a> {
    type T = f64;
    type LineStringType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<'a> MultiLineStringTrait for &'a MultiLineString<'a> {
    type T = f64;
    type LineStringType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<G: MultiLineStringTrait<T = f64>> PartialEq<G> for MultiLineString<'_> {
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::builder::MultiLineStringBuilder;
    use crate::test::multilinestring::{ml0, ml1};
    use crate::trait_::GeoArrowArrayAccessor;
    use geoarrow_schema::{CoordType, Dimension, MultiLineStringType};

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let typ =
            MultiLineStringType::new(CoordType::Interleaved, Dimension::XY, Default::default());

        let arr1 = MultiLineStringBuilder::from_multi_line_strings(
            vec![ml0(), ml1()].as_slice(),
            typ.clone(),
        )
        .finish();
        let arr2 =
            MultiLineStringBuilder::from_multi_line_strings(vec![ml0(), ml0()].as_slice(), typ)
                .finish();

        assert_eq!(arr1.value(0).unwrap(), arr2.value(0).unwrap());
        assert_ne!(arr1.value(1).unwrap(), arr2.value(1).unwrap());
    }
}
