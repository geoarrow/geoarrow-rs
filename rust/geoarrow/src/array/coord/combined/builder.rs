use core::f64;

use crate::array::{
    CoordBuffer, CoordType, InterleavedCoordBufferBuilder, SeparatedCoordBufferBuilder,
};
use crate::datatypes::Dimension;
use crate::error::Result;
use geo_traits::{CoordTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// Converting an [`CoordBufferBuilder`] into a [`CoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub enum CoordBufferBuilder {
    Interleaved(InterleavedCoordBufferBuilder),
    Separated(SeparatedCoordBufferBuilder),
}

impl CoordBufferBuilder {
    pub fn initialize(len: usize, interleaved: bool, dim: Dimension) -> Self {
        match interleaved {
            true => {
                CoordBufferBuilder::Interleaved(InterleavedCoordBufferBuilder::initialize(len, dim))
            }
            false => {
                CoordBufferBuilder::Separated(SeparatedCoordBufferBuilder::initialize(len, dim))
            }
        }
    }

    /// Reserves capacity for at least `additional` more coordinates to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.reserve(additional),
            CoordBufferBuilder::Separated(cb) => cb.reserve(additional),
        }
    }

    /// Reserves the minimum capacity for at least `additional` more coordinates to
    /// be inserted in the given `Vec<T>`. Unlike [`reserve`], this will not
    /// deliberately over-allocate to speculatively avoid frequent allocations.
    /// After calling `reserve_exact`, capacity will be greater than or equal to
    /// `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Vec::reserve
    pub fn reserve_exact(&mut self, additional: usize) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.reserve_exact(additional),
            CoordBufferBuilder::Separated(cb) => cb.reserve_exact(additional),
        }
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.capacity(),
            CoordBufferBuilder::Separated(cb) => cb.capacity(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.len(),
            CoordBufferBuilder::Separated(cb) => cb.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn coord_type(&self) -> CoordType {
        match self {
            CoordBufferBuilder::Interleaved(_) => CoordType::Interleaved,
            CoordBufferBuilder::Separated(_) => CoordType::Separated,
        }
    }

    /// Push a new coord onto the end of this coordinate buffer
    ///
    /// ## Panics
    ///
    /// - If the added coordinate does not have the same dimension as the coordinate buffer.
    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_coord(coord),
            CoordBufferBuilder::Separated(cb) => cb.push_coord(coord),
        }
    }

    /// Push a new coord onto the end of this coordinate buffer
    ///
    /// ## Errors
    ///
    /// - If the added coordinate does not have the same dimension as the coordinate buffer.
    pub fn try_push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> Result<()> {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.try_push_coord(coord),
            CoordBufferBuilder::Separated(cb) => cb.try_push_coord(coord),
        }
    }

    /// Push a valid coordinate with NaN values
    ///
    /// Used in the case of point and rect arrays, where a `null` array value still needs to have
    /// space allocated for it.
    pub fn push_nan_coord(&mut self) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_nan_coord(),
            CoordBufferBuilder::Separated(cb) => cb.push_nan_coord(),
        }
    }

    /// Push a new point onto the end of this coordinate buffer
    ///
    /// ## Panics
    ///
    /// - If the added point does not have the same dimension as the coordinate buffer.
    pub fn push_point(&mut self, point: &impl PointTrait<T = f64>) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_point(point),
            CoordBufferBuilder::Separated(cb) => cb.push_point(point),
        }
    }

    /// Push a new point onto the end of this coordinate buffer
    ///
    /// ## Errors
    ///
    /// - If the added point does not have the same dimension as the coordinate buffer.
    pub fn try_push_point(&mut self, point: &impl PointTrait<T = f64>) -> Result<()> {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.try_push_point(point),
            CoordBufferBuilder::Separated(cb) => cb.try_push_point(point),
        }
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_xy(x, y),
            CoordBufferBuilder::Separated(cb) => cb.push_xy(x, y),
        }
    }

    pub fn try_push_xy(&mut self, x: f64, y: f64) -> Result<()> {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.try_push_xy(x, y),
            CoordBufferBuilder::Separated(cb) => cb.try_push_xy(x, y),
        }
    }

    pub fn push_xyz(&mut self, x: f64, y: f64, z: f64) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_xyz(x, y, z),
            CoordBufferBuilder::Separated(cb) => cb.push_xyz(x, y, z),
        }
    }

    pub fn try_push_xyz(&mut self, x: f64, y: f64, z: f64) -> Result<()> {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.try_push_xyz(x, y, z),
            CoordBufferBuilder::Separated(cb) => cb.try_push_xyz(x, y, z),
        }
    }
}

impl From<CoordBufferBuilder> for CoordBuffer {
    fn from(value: CoordBufferBuilder) -> Self {
        match value {
            CoordBufferBuilder::Interleaved(cb) => CoordBuffer::Interleaved(cb.into()),
            CoordBufferBuilder::Separated(cb) => CoordBuffer::Separated(cb.into()),
        }
    }
}
