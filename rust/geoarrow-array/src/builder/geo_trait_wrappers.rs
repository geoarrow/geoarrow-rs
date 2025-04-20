//! Wrappers around `RectTrait`, `TriangleTrait`, and `LineTrait` to implement
//! `PolygonTrait`, `PolygonTrait` and `LineStringTrait` traits, respectively.
//!
//! This makes it easier to use `Rect`, `Triangle`, and `Line` types because we don't have to add
//! specialized code for them.

use geo_traits::{CoordTrait, LineStringTrait, LineTrait, PolygonTrait, RectTrait, TriangleTrait};
use wkt::WktNum;

use crate::error::{GeoArrowError, Result};

pub(crate) struct RectWrapper<'a, T: WktNum, R: RectTrait<T = T>>(&'a R);

impl<'a, T: WktNum, R: RectTrait<T = T>> RectWrapper<'a, T, R> {
    pub(crate) fn try_new(rect: &'a R) -> Result<Self> {
        match rect.dim() {
            geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {}
            _ => {
                return Err(GeoArrowError::General(
                    "Only 2d rect supported when pushing to polygon.".to_string(),
                ));
            }
        };

        Ok(Self(rect))
    }

    fn ll(&self) -> wkt::types::Coord<T> {
        let lower = self.0.min();
        wkt::types::Coord {
            x: lower.x(),
            y: lower.y(),
            z: None,
            m: None,
        }
    }

    fn ul(&self) -> wkt::types::Coord<T> {
        let lower = self.0.min();
        let upper = self.0.max();
        wkt::types::Coord {
            x: lower.x(),
            y: upper.y(),
            z: None,
            m: None,
        }
    }

    fn ur(&self) -> wkt::types::Coord<T> {
        let upper = self.0.max();
        wkt::types::Coord {
            x: upper.x(),
            y: upper.y(),
            z: None,
            m: None,
        }
    }

    fn lr(&self) -> wkt::types::Coord<T> {
        let lower = self.0.min();
        let upper = self.0.max();
        wkt::types::Coord {
            x: upper.x(),
            y: lower.y(),
            z: None,
            m: None,
        }
    }
}

impl<T: WktNum, R: RectTrait<T = T>> PolygonTrait for RectWrapper<'_, T, R> {
    type T = T;
    type RingType<'a>
        = &'a RectWrapper<'a, T, R>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        Some(self)
    }

    fn num_interiors(&self) -> usize {
        0
    }

    unsafe fn interior_unchecked(&self, _: usize) -> Self::RingType<'_> {
        panic!("interior_unchecked called on a rect")
    }
}
impl<'a, T: WktNum, R: RectTrait<T = T>> LineStringTrait for &'a RectWrapper<'a, T, R> {
    type T = T;
    type CoordType<'b>
        = wkt::types::Coord<T>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
    }

    fn num_coords(&self) -> usize {
        5
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        // Ref below because I always forget the ordering
        // https://github.com/georust/geo/blob/76ad2a358bd079e9d47b1229af89608744d2635b/geo-types/src/geometry/rect.rs#L217-L225
        match i {
            0 => self.ll(),
            1 => self.ul(),
            2 => self.ur(),
            3 => self.lr(),
            4 => self.ll(),
            _ => panic!("out of range for rect coord: {i}"),
        }
    }
}

pub(crate) struct TriangleWrapper<'a, T, Tri: TriangleTrait<T = T>>(pub(crate) &'a Tri);

impl<T, Tri: TriangleTrait<T = T>> PolygonTrait for TriangleWrapper<'_, T, Tri> {
    type T = T;
    type RingType<'a>
        = &'a TriangleWrapper<'a, T, Tri>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        Some(self)
    }

    fn num_interiors(&self) -> usize {
        0
    }

    unsafe fn interior_unchecked(&self, _: usize) -> Self::RingType<'_> {
        panic!("interior_unchecked called on a triangle")
    }
}

impl<'a, T, Tri: TriangleTrait<T = T>> LineStringTrait for &'a TriangleWrapper<'a, T, Tri> {
    type T = T;
    type CoordType<'b>
        = <Tri as TriangleTrait>::CoordType<'b>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
    }

    fn num_coords(&self) -> usize {
        4
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        match i {
            0 => self.0.first(),
            1 => self.0.second(),
            2 => self.0.third(),
            3 => self.0.first(),
            _ => panic!("out of range for triangle ring: {i}"),
        }
    }
}

pub(crate) struct LineWrapper<'a, T, L: LineTrait<T = T>>(pub(crate) &'a L);

impl<T, L: LineTrait<T = T>> LineStringTrait for LineWrapper<'_, T, L> {
    type T = T;
    type CoordType<'b>
        = <L as LineTrait>::CoordType<'b>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
    }

    fn num_coords(&self) -> usize {
        2
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        match i {
            0 => self.0.start(),
            1 => self.0.end(),
            _ => panic!("out of range for line coord: {i}"),
        }
    }
}
