use core::f64;

use crate::array::SeparatedCoordBuffer;
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
use geo_traits::{CoordTrait, PointTrait};

/// The GeoArrow equivalent to `Vec<Coord>`: a mutable collection of coordinates.
///
/// This stores all coordinates in separated fashion as multiple arrays: `xxx` and `yyy`.
///
/// Converting an [`SeparatedCoordBufferBuilder`] into a [`SeparatedCoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct SeparatedCoordBufferBuilder {
    buffers: [Vec<f64>; 4],
    dim: Dimension,
}

impl SeparatedCoordBufferBuilder {
    // TODO: switch this new (initializing to zero) to default?
    pub fn new(dim: Dimension) -> Self {
        Self::with_capacity(0, dim)
    }

    // TODO: need to figure out how to take variable-length input but take ownership from the
    // input.
    // pub fn from_vecs(buffers: &[Vec<f64>], dim: Dimension) -> Result<Self> {
    //     if buffers.len() != dim.size() {
    //         return Err(GeoArrowError::General(
    //             "Buffers must match dimension length ".into(),
    //         ));
    //     }

    //     // Fill buffers with empty buffers past needed dimensions
    //     let buffers = core::array::from_fn(|i| {
    //         if i < buffers.len() {
    //             buffers[0]
    //         } else {
    //             Vec::new()
    //         }
    //     });

    //     Self { buffers }
    // }

    pub fn with_capacity(capacity: usize, dim: Dimension) -> Self {
        // Only allocate buffers for existant dimensions
        let buffers = core::array::from_fn(|i| {
            if i < dim.size() {
                Vec::with_capacity(capacity)
            } else {
                Vec::new()
            }
        });

        Self { buffers, dim }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize, dim: Dimension) -> Self {
        // Only allocate buffers for existant dimensions
        let buffers = core::array::from_fn(|i| {
            if i < dim.size() {
                vec![0.0f64; len]
            } else {
                Vec::new()
            }
        });

        Self { buffers, dim }
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

        self.buffers[0].push(coord.x());
        self.buffers[1].push(coord.y());
        if let Some(z) = coord.nth(2) {
            self.buffers[2].push(z);
        };
        Ok(())
    }

    /// Push a valid coordinate with NaN values
    ///
    /// Used in the case of point and rect arrays, where a `null` array value still needs to have
    /// space allocated for it.
    pub fn push_nan_coord(&mut self) {
        for i in 0..self.dim.size() {
            self.buffers[i].push(f64::NAN);
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

        self.buffers[0].push(x);
        self.buffers[1].push(y);
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

        self.buffers[0].push(x);
        self.buffers[1].push(y);
        self.buffers[2].push(z);
        Ok(())
    }

    pub fn from_coords<G: CoordTrait<T = f64>>(coords: &[G], dim: Dimension) -> Result<Self> {
        let mut buffer = SeparatedCoordBufferBuilder::with_capacity(coords.len(), dim);
        for coord in coords {
            buffer.try_push_coord(coord)?;
        }
        Ok(buffer)
    }
}

impl From<SeparatedCoordBufferBuilder> for SeparatedCoordBuffer {
    fn from(value: SeparatedCoordBufferBuilder) -> Self {
        // Initialize buffers with empty array, then mutate into it
        let mut buffers = core::array::from_fn(|_| vec![].into());
        for (i, buffer) in value.buffers.into_iter().enumerate() {
            buffers[i] = buffer.into();
        }
        SeparatedCoordBuffer::new(buffers, value.dim)
    }
}
