use core::f64;

use crate::array::SeparatedCoordBuffer;
use geo_traits::{CoordTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// This stores all coordinates in separated fashion as multiple arrays: `xxx` and `yyy`.
///
/// Converting an [`SeparatedCoordBufferBuilder`] into a [`SeparatedCoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct SeparatedCoordBufferBuilder<const D: usize> {
    buffers: [Vec<f64>; D],
}

impl<const D: usize> SeparatedCoordBufferBuilder<D> {
    // TODO: switch this new (initializing to zero) to default?
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn from_vecs(buffers: [Vec<f64>; D]) -> Self {
        Self { buffers }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffers: core::array::from_fn(|_| Vec::with_capacity(capacity)),
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize) -> Self {
        Self {
            buffers: core::array::from_fn(|_| vec![0.0f64; len]),
        }
    }

    /// Reserves capacity for at least `additional` more coordinates to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.buffers
            .iter_mut()
            .for_each(|buffer| buffer.reserve(additional))
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
        self.buffers
            .iter_mut()
            .for_each(|buffer| buffer.reserve_exact(additional))
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.buffers[0].capacity()
    }

    pub fn len(&self) -> usize {
        self.buffers[0].len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, c: [f64; D]) {
        for (i, value) in c.iter().enumerate().take(D) {
            self.buffers[i].push(*value);
        }
    }

    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        // TODO: how to handle when coord dimensions and store dimensions don't line up?
        for (i, buffer) in self.buffers.iter_mut().enumerate() {
            buffer.push(coord.nth(i).unwrap_or(f64::NAN))
        }
    }

    pub fn push_point(&mut self, point: &impl PointTrait<T = f64>) {
        if let Some(coord) = point.coord() {
            self.push_coord(&coord);
        } else {
            self.push([f64::NAN; D]);
        }
    }
}

impl SeparatedCoordBufferBuilder<2> {
    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        self.buffers[0][i] = coord.x;
        self.buffers[1][i] = coord.y;
    }

    pub fn set_xy(&mut self, i: usize, x: f64, y: f64) {
        self.buffers[0][i] = x;
        self.buffers[1][i] = y;
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.buffers[0].push(x);
        self.buffers[1].push(y);
    }
}

impl<const D: usize> Default for SeparatedCoordBufferBuilder<D> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<const D: usize> From<SeparatedCoordBufferBuilder<D>> for SeparatedCoordBuffer<D> {
    fn from(value: SeparatedCoordBufferBuilder<D>) -> Self {
        // Initialize buffers with empty array, then mutate into it
        let mut buffers = core::array::from_fn(|_| vec![].into());
        for (i, buffer) in value.buffers.into_iter().enumerate() {
            buffers[i] = buffer.into();
        }
        SeparatedCoordBuffer::new(buffers)
    }
}

impl<G: CoordTrait<T = f64>, const D: usize> From<&[G]> for SeparatedCoordBufferBuilder<D> {
    fn from(value: &[G]) -> Self {
        let mut buffer = SeparatedCoordBufferBuilder::with_capacity(value.len());
        for coord in value {
            buffer.push_coord(coord);
        }
        buffer
    }
}
