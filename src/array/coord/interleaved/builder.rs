use crate::array::InterleavedCoordBuffer;
use crate::geo_traits::CoordTrait;

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// This stores all coordinates in interleaved fashion as `xyxyxy`.
///
/// Converting an [`InterleavedCoordBufferBuilder`] into a [`InterleavedCoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct InterleavedCoordBufferBuilder {
    pub coords: Vec<f64>,
}

impl InterleavedCoordBufferBuilder {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            coords: Vec::with_capacity(capacity * 2),
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize) -> Self {
        Self {
            coords: vec![0.0f64; len * 2],
        }
    }

    /// Reserves capacity for at least `additional` more coordinates to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.coords.reserve(additional * 2);
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
        self.coords.reserve_exact(additional * 2);
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.coords.capacity() / 2
    }

    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        self.coords[i * 2] = coord.x;
        self.coords[i * 2 + 1] = coord.y;
    }

    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        self.coords.push(coord.x());
        self.coords.push(coord.y());
    }

    pub fn set_xy(&mut self, i: usize, x: f64, y: f64) {
        self.coords[i * 2] = x;
        self.coords[i * 2 + 1] = y;
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.coords.push(x);
        self.coords.push(y);
    }

    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for InterleavedCoordBufferBuilder {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl From<InterleavedCoordBufferBuilder> for InterleavedCoordBuffer {
    fn from(value: InterleavedCoordBufferBuilder) -> Self {
        InterleavedCoordBuffer::new(value.coords.into())
    }
}

impl<G: CoordTrait<T = f64>> From<&[G]> for InterleavedCoordBufferBuilder {
    fn from(value: &[G]) -> Self {
        let mut buffer = InterleavedCoordBufferBuilder::with_capacity(value.len());
        for coord in value {
            buffer.push_coord(coord);
        }
        buffer
    }
}
