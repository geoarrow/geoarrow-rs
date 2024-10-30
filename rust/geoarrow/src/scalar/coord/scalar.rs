use geo_traits::CoordTrait;

use crate::array::CoordBuffer;

pub struct Coord<'a> {
    /// The underlying coord buffer
    pub(crate) buffer: &'a CoordBuffer,

    /// The index within the buffer
    pub(crate) i: usize,
}

impl<'a> Coord<'a> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        (0..self.dim().size()).all(|coord_dim| self.nth_unchecked(coord_dim).is_nan())
    }
}

impl<'a> CoordTrait for Coord<'a> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        self.buffer.dim.into()
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        self.buffer.buffers[n][self.i * self.buffer.coords_stride]
    }

    fn x(&self) -> Self::T {
        self.buffer.buffers[0][self.i * self.buffer.coords_stride]
    }

    fn y(&self) -> Self::T {
        self.buffer.buffers[1][self.i * self.buffer.coords_stride]
    }
}
