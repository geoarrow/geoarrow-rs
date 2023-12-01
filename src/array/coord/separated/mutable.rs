use crate::array::SeparatedCoordBuffer;
use crate::geo_traits::CoordTrait;

#[derive(Debug, Clone)]
pub struct SeparatedCoordBufferBuilder {
    x: Vec<f64>,
    y: Vec<f64>,
}

impl SeparatedCoordBufferBuilder {
    // TODO: switch this new (initializing to zero) to default?
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn from_vecs(x: Vec<f64>, y: Vec<f64>) -> Self {
        Self { x, y }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(capacity),
            y: Vec::with_capacity(capacity),
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize) -> Self {
        Self {
            x: vec![0.0f64; len],
            y: vec![0.0f64; len],
        }
    }

    /// Reserves capacity for at least `additional` more coordinates to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.x.reserve(additional);
        self.y.reserve(additional);
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
        self.x.reserve_exact(additional);
        self.y.reserve_exact(additional);
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.x.capacity()
    }

    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        self.x[i] = coord.x;
        self.y[i] = coord.y;
    }

    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        self.x.push(coord.x());
        self.y.push(coord.y());
    }

    pub fn set_xy(&mut self, i: usize, x: f64, y: f64) {
        self.x[i] = x;
        self.y[i] = y;
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.x.push(x);
        self.y.push(y);
    }

    pub fn len(&self) -> usize {
        self.x.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SeparatedCoordBufferBuilder {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl From<SeparatedCoordBufferBuilder> for SeparatedCoordBuffer {
    fn from(value: SeparatedCoordBufferBuilder) -> Self {
        SeparatedCoordBuffer::new(value.x.into(), value.y.into())
    }
}

impl<G: CoordTrait<T = f64>> From<&[G]> for SeparatedCoordBufferBuilder {
    fn from(value: &[G]) -> Self {
        let mut buffer = SeparatedCoordBufferBuilder::with_capacity(value.len());
        for coord in value {
            buffer.push_coord(coord);
        }
        buffer
    }
}
