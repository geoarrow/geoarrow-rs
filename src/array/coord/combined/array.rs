use crate::array::{
    CoordType, InterleavedCoordBuffer, MutableInterleavedCoordBuffer, MutableSeparatedCoordBuffer,
    SeparatedCoordBuffer,
};
use crate::error::GeoArrowError;
use crate::scalar::Coord;
use crate::GeometryArrayTrait;
use arrow2::array::{Array, FixedSizeListArray, StructArray};
use arrow2::datatypes::DataType;
use itertools::Itertools;
use rstar::RTree;

/// An Arrow representation of an array of coordinates.
///
/// As defined in the GeoArrow spec, coordinates can either be interleaved (i.e. a single array of
/// XYXYXY) or separated (i.e. two arrays, one XXX and another YYY).
///
/// This CoordBuffer abstracts over an `InterleavedCoordBuffer` and a `SeparatedCoordBuffer`.
///
/// For now all coordinate buffers support only two dimensions.
///
/// This is named `CoordBuffer` instead of `CoordArray` because the buffer does not store its own
/// validity bitmask. Rather the geometry arrays that build on top of this maintain their own
/// validity masks.
#[derive(Debug, Clone)]
pub enum CoordBuffer {
    Interleaved(InterleavedCoordBuffer),
    Separated(SeparatedCoordBuffer),
}

impl CoordBuffer {
    pub fn get_x(&self, i: usize) -> f64 {
        let geo_coord: geo::Coord = self.value(i).into();
        geo_coord.x
    }

    pub fn get_y(&self, i: usize) -> f64 {
        let geo_coord: geo::Coord = self.value(i).into();
        geo_coord.y
    }
}

impl<'a> GeometryArrayTrait<'a> for CoordBuffer {
    type ArrowArray = Box<dyn Array>;
    type Scalar = Coord<'a>;
    type ScalarGeo = geo::Coord;
    type RTreeObject = Self::Scalar;

    fn value(&'a self, i: usize) -> Self::Scalar {
        match self {
            CoordBuffer::Interleaved(c) => Coord::Interleaved(c.value(i)),
            CoordBuffer::Separated(c) => Coord::Separated(c.value(i)),
        }
    }

    fn logical_type(&self) -> DataType {
        match self {
            CoordBuffer::Interleaved(c) => c.logical_type(),
            CoordBuffer::Separated(c) => c.logical_type(),
        }
    }

    fn extension_type(&self) -> DataType {
        panic!("Coordinate arrays do not have an extension name.")
    }

    fn into_arrow(self) -> Self::ArrowArray {
        match self {
            CoordBuffer::Interleaved(c) => c.into_arrow().boxed(),
            CoordBuffer::Separated(c) => c.into_arrow().boxed(),
        }
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow()
    }

    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.len());
        coords
    }

    fn coord_type(&self) -> CoordType {
        match self {
            CoordBuffer::Interleaved(cb) => cb.coord_type(),
            CoordBuffer::Separated(cb) => cb.coord_type(),
        }
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        match (self, coord_type) {
            (CoordBuffer::Interleaved(cb), CoordType::Interleaved) => CoordBuffer::Interleaved(cb),
            (CoordBuffer::Interleaved(cb), CoordType::Separated) => {
                let mut new_buffer = MutableSeparatedCoordBuffer::with_capacity(cb.len());
                cb.coords
                    .into_iter()
                    .tuples()
                    .for_each(|(x, y)| new_buffer.push_xy(x, y));
                CoordBuffer::Separated(new_buffer.into())
            }
            (CoordBuffer::Separated(cb), CoordType::Separated) => CoordBuffer::Separated(cb),
            (CoordBuffer::Separated(cb), CoordType::Interleaved) => {
                let mut new_buffer = MutableInterleavedCoordBuffer::with_capacity(cb.len());
                cb.x.into_iter()
                    .zip(cb.y)
                    .for_each(|(x, y)| new_buffer.push_xy(x, y));
                CoordBuffer::Interleaved(new_buffer.into())
            }
        }
    }

    fn rstar_tree(&'a self) -> RTree<Self::Scalar> {
        panic!("not implemented for coords");
    }

    fn len(&self) -> usize {
        match self {
            CoordBuffer::Interleaved(c) => c.len(),
            CoordBuffer::Separated(c) => c.len(),
        }
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&mut self, offset: usize, length: usize) {
        match self {
            CoordBuffer::Interleaved(c) => c.slice(offset, length),
            CoordBuffer::Separated(c) => c.slice(offset, length),
        };
    }

    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        match self {
            CoordBuffer::Interleaved(c) => c.slice_unchecked(offset, length),
            CoordBuffer::Separated(c) => c.slice_unchecked(offset, length),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        match self {
            CoordBuffer::Interleaved(cb) => {
                CoordBuffer::Interleaved(cb.owned_slice(offset, length))
            }
            CoordBuffer::Separated(cb) => CoordBuffer::Separated(cb.owned_slice(offset, length)),
        }
    }

    fn to_boxed(&self) -> Box<Self> {
        todo!()
        // match self {
        //     CoordBuffer::Interleaved(c) => self.to_boxed(),
        //     CoordBuffer::Separated(c) => self.to_boxed(),
        // }
    }
}

impl TryFrom<&dyn Array> for CoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type().to_logical_type() {
            DataType::Struct(_) => {
                let downcasted = value.as_any().downcast_ref::<StructArray>().unwrap();
                Ok(CoordBuffer::Separated(downcasted.try_into()?))
            }
            DataType::FixedSizeList(_, _) => {
                let downcasted = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                Ok(CoordBuffer::Interleaved(downcasted.try_into()?))
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl PartialEq for CoordBuffer {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CoordBuffer::Interleaved(a), CoordBuffer::Interleaved(b)) => PartialEq::eq(a, b),
            (CoordBuffer::Interleaved(left), CoordBuffer::Separated(right)) => {
                if left.len() != right.len() {
                    return false;
                }

                for i in 0..left.len() {
                    let left_coord = left.value(i);
                    let right_coord = right.value(i);

                    if left_coord != right_coord {
                        return false;
                    }
                }

                true
            }
            (CoordBuffer::Separated(a), CoordBuffer::Separated(b)) => PartialEq::eq(a, b),
            (CoordBuffer::Separated(left), CoordBuffer::Interleaved(right)) => {
                if left.len() != right.len() {
                    return false;
                }

                for i in 0..left.len() {
                    let left_coord = left.value(i);
                    let right_coord = right.value(i);

                    if left_coord != right_coord {
                        return false;
                    }
                }

                true
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::error::Result;

    use super::*;

    #[test]
    fn test_eq_both_interleaved() -> Result<()> {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = CoordBuffer::Interleaved(coords1.try_into()?);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = CoordBuffer::Interleaved(coords2.try_into()?);

        assert_eq!(buf1, buf2);
        Ok(())
    }

    #[test]
    fn test_eq_across_types() -> Result<()> {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = CoordBuffer::Separated((x1, y1).try_into()?);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = CoordBuffer::Interleaved(coords2.try_into()?);

        assert_eq!(buf1, buf2);
        Ok(())
    }

    #[test]
    fn test_eq_across_types_slicing() -> Result<()> {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let mut buf1 = CoordBuffer::Separated((x1, y1).try_into()?);
        buf1.slice(1, 1);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let mut buf2 = CoordBuffer::Interleaved(coords2.try_into()?);
        buf2.slice(1, 1);

        assert_eq!(buf1, buf2);
        Ok(())
    }
}
