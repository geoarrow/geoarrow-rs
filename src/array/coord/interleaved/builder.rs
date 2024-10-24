use crate::array::InterleavedCoordBuffer;
use crate::geo_traits::{CoordTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// This stores all coordinates in interleaved fashion as `xyxyxy`.
///
/// Converting an [`InterleavedCoordBufferBuilder`] into a [`InterleavedCoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct InterleavedCoordBufferBuilder<const D: usize> {
    pub coords: Vec<f64>,
}

impl<const D: usize> InterleavedCoordBufferBuilder<D> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            coords: Vec::with_capacity(capacity * D),
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize) -> Self {
        Self {
            coords: vec![0.0f64; len * D],
        }
    }

    /// Reserves capacity for at least `additional` more coordinates to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.coords.reserve(additional * D);
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
        self.coords.reserve_exact(additional * D);
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.coords.capacity() / D
    }

    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, c: [f64; D]) {
        self.coords.extend_from_slice(&c);
    }

    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        let buf: [f64; D] = core::array::from_fn(|i| coord.nth(i).unwrap_or(f64::NAN));
        self.coords.extend_from_slice(&buf);
    }

    pub fn push_point(&mut self, point: &impl PointTrait<T = f64>) {
        if let Some(coord) = point.coord() {
            self.push_coord(&coord);
        } else {
            self.push([f64::NAN; D]);
        }
    }
}

impl InterleavedCoordBufferBuilder<2> {
    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        self.coords[i * 2] = coord.x;
        self.coords[i * 2 + 1] = coord.y;
    }

    pub fn set_xy(&mut self, i: usize, x: f64, y: f64) {
        self.coords[i * 2] = x;
        self.coords[i * 2 + 1] = y;
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.coords.push(x);
        self.coords.push(y);
    }
}

impl<const D: usize> Default for InterleavedCoordBufferBuilder<D> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<const D: usize> From<InterleavedCoordBufferBuilder<D>> for InterleavedCoordBuffer<D> {
    fn from(value: InterleavedCoordBufferBuilder<D>) -> Self {
        InterleavedCoordBuffer::new(value.coords.into())
    }
}

impl<G: PointTrait<T = f64>, const D: usize> From<&[G]> for InterleavedCoordBufferBuilder<D> {
    fn from(value: &[G]) -> Self {
        let mut buffer = InterleavedCoordBufferBuilder::with_capacity(value.len());
        for coord in value {
            buffer.push_point(coord);
        }
        buffer
    }
}
