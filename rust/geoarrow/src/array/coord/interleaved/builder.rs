use core::f64;

use crate::array::InterleavedCoordBuffer;
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
use geo_traits::{CoordTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// This stores all coordinates in interleaved fashion as `xyxyxy`.
///
/// Converting an [`InterleavedCoordBufferBuilder`] into a [`InterleavedCoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct InterleavedCoordBufferBuilder {
    pub coords: Vec<f64>,
    dim: Dimension,
}

impl InterleavedCoordBufferBuilder {
    pub fn new(dim: Dimension) -> Self {
        Self::with_capacity(0, dim)
    }

    pub fn with_capacity(capacity: usize, dim: Dimension) -> Self {
        Self {
            coords: Vec::with_capacity(capacity * dim.size()),
            dim,
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize, dim: Dimension) -> Self {
        Self {
            coords: vec![0.0f64; len * dim.size()],
            dim,
        }
    }

    /// Reserves capacity for at least `additional` more coordinates to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.coords.reserve(additional * self.dim.size());
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
        self.coords.reserve_exact(additional * self.dim.size());
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.coords.capacity() / self.dim.size()
    }

    pub fn len(&self) -> usize {
        self.coords.len() / self.dim.size()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push a new coord onto the end of this coordinate buffer
    ///
    /// ## Panics
    ///
    /// - If the added coordinate does not have the same dimension as the coordinate buffer.
    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        self.try_push_coord(coord).unwrap()
    }

    /// Push a new coord onto the end of this coordinate buffer
    ///
    /// ## Errors
    ///
    /// - If the added coordinate does not have the same dimension as the coordinate buffer.
    pub fn try_push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> Result<()> {
        // TODO: should check xyz/zym
        if coord.dim().size() != self.dim.size() {
            return Err(GeoArrowError::General(
                "coord dimension must match coord buffer dimension.".into(),
            ));
        }

        self.coords.push(coord.x());
        self.coords.push(coord.y());
        if let Some(z) = coord.nth(2) {
            self.coords.push(z);
        };
        Ok(())
    }

    /// Push a valid coordinate with NaN values
    ///
    /// Used in the case of point and rect arrays, where a `null` array value still needs to have
    /// space allocated for it.
    pub fn push_nan_coord(&mut self) {
        for _ in 0..self.dim.size() {
            self.coords.push(f64::NAN);
        }
    }

    /// Push a new point onto the end of this coordinate buffer
    ///
    /// ## Panics
    ///
    /// - If the added point does not have the same dimension as the coordinate buffer.
    pub fn push_point(&mut self, point: &impl PointTrait<T = f64>) {
        self.try_push_point(point).unwrap()
    }

    /// Push a new point onto the end of this coordinate buffer
    ///
    /// ## Errors
    ///
    /// - If the added point does not have the same dimension as the coordinate buffer.
    pub fn try_push_point(&mut self, point: &impl PointTrait<T = f64>) -> Result<()> {
        if let Some(coord) = point.coord() {
            self.try_push_coord(&coord)?;
        } else {
            self.push_nan_coord();
        };
        Ok(())
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.try_push_xy(x, y).unwrap()
    }

    pub fn try_push_xy(&mut self, x: f64, y: f64) -> Result<()> {
        if !matches!(self.dim, Dimension::XY) {
            return Err(GeoArrowError::General(format!(
                "Tried to push xy but internal dimension is {:?}.",
                self.dim
            )));
        }

        self.coords.push(x);
        self.coords.push(y);
        Ok(())
    }

    pub fn push_xyz(&mut self, x: f64, y: f64, z: f64) {
        self.try_push_xyz(x, y, z).unwrap()
    }

    pub fn try_push_xyz(&mut self, x: f64, y: f64, z: f64) -> Result<()> {
        if !matches!(self.dim, Dimension::XYZ) {
            return Err(GeoArrowError::General(format!(
                "Tried to push xyz but internal dimension is {:?}.",
                self.dim
            )));
        }

        self.coords.push(x);
        self.coords.push(y);
        self.coords.push(z);
        Ok(())
    }

    pub fn from_coords<G: CoordTrait<T = f64>>(coords: &[G], dim: Dimension) -> Result<Self> {
        let mut buffer = InterleavedCoordBufferBuilder::with_capacity(coords.len(), dim);
        for coord in coords {
            buffer.push_coord(coord);
        }
        Ok(buffer)
    }
}

impl From<InterleavedCoordBufferBuilder> for InterleavedCoordBuffer {
    fn from(value: InterleavedCoordBufferBuilder) -> Self {
        InterleavedCoordBuffer::new(value.coords.into(), value.dim)
    }
}
