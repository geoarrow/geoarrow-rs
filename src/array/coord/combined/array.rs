use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
use crate::scalar::Coord;
use crate::GeometryArrayTrait;
use arrow2::array::Array;
use arrow2::datatypes::DataType;

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

    fn to_boxed(&self) -> Box<Self> {
        todo!()
        // match self {
        //     CoordBuffer::Interleaved(c) => self.to_boxed(),
        //     CoordBuffer::Separated(c) => self.to_boxed(),
        // }
    }
}
