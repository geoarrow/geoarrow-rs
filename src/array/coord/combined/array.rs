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
#[derive(Debug, Clone, PartialEq)]
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
                    .zip(cb.y.into_iter())
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
