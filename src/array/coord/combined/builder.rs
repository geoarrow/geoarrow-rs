use core::f64;

use crate::array::{
    CoordBuffer, CoordType, InterleavedCoordBufferBuilder, SeparatedCoordBufferBuilder,
};
use crate::geo_traits::{CoordTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// Converting an [`CoordBufferBuilder`] into a [`CoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub enum CoordBufferBuilder<const D: usize> {
    Interleaved(InterleavedCoordBufferBuilder<D>),
    Separated(SeparatedCoordBufferBuilder<D>),
}

impl<const D: usize> CoordBufferBuilder<D> {
    pub fn initialize(len: usize, interleaved: bool) -> Self {
        match interleaved {
            true => CoordBufferBuilder::Interleaved(InterleavedCoordBufferBuilder::initialize(len)),
            false => CoordBufferBuilder::Separated(SeparatedCoordBufferBuilder::initialize(len)),
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

    pub fn push(&mut self, c: [f64; D]) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push(c),
            CoordBufferBuilder::Separated(cb) => cb.push(c),
        }
    }

    pub fn coord_type(&self) -> CoordType {
        match self {
            CoordBufferBuilder::Interleaved(_) => CoordType::Interleaved,
            CoordBufferBuilder::Separated(_) => CoordType::Separated,
        }
    }

    // TODO: how should this handle coords that don't have the same dimension D?
    pub fn push_coord(&mut self, point: &impl CoordTrait<T = f64>) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_coord(point),
            CoordBufferBuilder::Separated(cb) => cb.push_coord(point),
        }
    }

    // TODO: how should this handle coords that don't have the same dimension D?
    pub fn push_point(&mut self, point: &impl PointTrait<T = f64>) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_point(point),
            CoordBufferBuilder::Separated(cb) => cb.push_point(point),
        }
    }
}

impl CoordBufferBuilder<2> {
    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.set_coord(i, coord),
            CoordBufferBuilder::Separated(cb) => cb.set_coord(i, coord),
        }
    }

    pub fn set_xy(&mut self, i: usize, x: f64, y: f64) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.set_xy(i, x, y),
            CoordBufferBuilder::Separated(cb) => cb.set_xy(i, x, y),
        }
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        match self {
            CoordBufferBuilder::Interleaved(cb) => cb.push_xy(x, y),
            CoordBufferBuilder::Separated(cb) => cb.push_xy(x, y),
        }
    }
}

impl<const D: usize> From<CoordBufferBuilder<D>> for CoordBuffer<D> {
    fn from(value: CoordBufferBuilder<D>) -> Self {
        match value {
            CoordBufferBuilder::Interleaved(cb) => CoordBuffer::Interleaved(cb.into()),
            CoordBufferBuilder::Separated(cb) => CoordBuffer::Separated(cb.into()),
        }
    }
}
