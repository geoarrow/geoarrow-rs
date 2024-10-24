use std::marker::PhantomData;

use super::iterator::LineStringIterator;
use super::{CoordTrait, Dimensions, UnimplementedCoord};
use geo::{Coord, CoordNum, LineString};

/// A trait for accessing data from a generic LineString.
///
/// A LineString is an ordered collection of two or more [points][CoordTrait], representing a path
/// between locations.
///
/// Refer to [geo_types::LineString] for information about semantics and validity.
pub trait LineStringTrait: Sized {
    /// The coordinate type of this geometry
    type T: CoordNum;

    /// The type of each underlying coordinate, which implements [CoordTrait]
    type CoordType<'a>: 'a + CoordTrait<T = Self::T>
    where
        Self: 'a;

    /// The dimension of this geometry
    fn dim(&self) -> Dimensions;

    /// An iterator over the coordinates in this LineString
    fn coords(&self) -> impl DoubleEndedIterator + ExactSizeIterator<Item = Self::CoordType<'_>> {
        LineStringIterator::new(self, 0, self.num_coords())
    }

    /// The number of coordinates in this LineString
    fn num_coords(&self) -> usize;

    /// Access to a specified coordinate in this LineString
    /// Will return None if the provided index is out of bounds
    #[inline]
    fn coord(&self, i: usize) -> Option<Self::CoordType<'_>> {
        if i >= self.num_coords() {
            None
        } else {
            unsafe { Some(self.coord_unchecked(i)) }
        }
    }

    /// Access to a specified coordinate in this LineString
    ///
    /// # Safety
    ///
    /// Accessing an index out of bounds is UB.
    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_>;
}

impl<T: CoordNum> LineStringTrait for LineString<T> {
    type T = T;
    type CoordType<'a> = &'a Coord<Self::T> where Self: 'a;

    fn dim(&self) -> Dimensions {
        Dimensions::Xy
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.0.get_unchecked(i)
    }
}

impl<'a, T: CoordNum> LineStringTrait for &'a LineString<T> {
    type T = T;
    type CoordType<'b> = &'a Coord<Self::T> where Self: 'b;

    fn dim(&self) -> Dimensions {
        Dimensions::Xy
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.0.get_unchecked(i)
    }
}

/// An empty struct that implements [LineStringTrait].
///
/// This can be used as the `LineStringType` of the `GeometryTrait` by implementations that don't
/// have a LineString concept
pub struct UnimplementedLineString<T: CoordNum>(PhantomData<T>);

impl<T: CoordNum> LineStringTrait for UnimplementedLineString<T> {
    type T = T;
    type CoordType<'a> = UnimplementedCoord<Self::T> where Self: 'a;

    fn dim(&self) -> Dimensions {
        unimplemented!()
    }

    fn num_coords(&self) -> usize {
        unimplemented!()
    }

    unsafe fn coord_unchecked(&self, _i: usize) -> Self::CoordType<'_> {
        unimplemented!()
    }
}
