use super::point::PointTrait;
use geo::{CoordNum, MultiPoint, Point};
use std::slice::Iter;

pub struct MultiPointIterator<
    'a,
    T: CoordNum,
    ItemType: 'a + PointTrait<T = T>,
    G: MultiPointTrait2<T = T, ItemType<'a> = ItemType>,
> {
    geom: &'a G,
    index: usize,
    end: usize,
}

impl<
        'a,
        T: CoordNum,
        ItemType: 'a + PointTrait<T = T>,
        G: MultiPointTrait2<T = T, ItemType<'a> = ItemType>,
    > Iterator for MultiPointIterator<'a, T, ItemType, G>
{
    type Item = ItemType;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.point(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<
        'a,
        T: CoordNum,
        ItemType: 'a + PointTrait<T = T>,
        G: MultiPointTrait2<T = T, ItemType<'a> = ItemType>,
    > DoubleEndedIterator for MultiPointIterator<'a, T, ItemType, G> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.point(self.end)
        }
    }
}

pub trait MultiPointTrait2: Sized {
    type T: CoordNum;
    type ItemType<'a>: 'a + PointTrait<T = Self::T>
    where
        Self: 'a;

    /// An iterator over the points in this MultiPoint
    fn points(&self) -> MultiPointIterator<'_, Self::T, Self::ItemType<'_>, Self> {
        MultiPointIterator {
            geom: self,
            index: 0,
            end: self.num_points(),
        }
    }

    /// The number of points in this MultiPoint
    fn num_points(&self) -> usize;

    /// Access to a specified point in this MultiPoint
    /// Will return None if the provided index is out of bounds
    fn point(&self, i: usize) -> Option<Self::ItemType<'_>>;
}

impl<T: CoordNum> MultiPointTrait2 for MultiPoint<T> {
    type T = T;
    type ItemType<'a> = &'a Point<Self::T> where Self: 'a;

    fn num_points(&self) -> usize {
        self.0.len()
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i)
    }
}

#[test]
fn tmp() {
    let mp = MultiPoint::new(vec![
        Point::new(0.0, 1.0),
        Point::new(2.0, 3.0),
        Point::new(4.0, 5.0),
    ]);
    MultiPointTrait2::points(&mp).for_each(|p| {
        dbg!(p);
    });
}

pub trait MultiPointTrait {
    type T: CoordNum;
    type ItemType<'a>: 'a + PointTrait<T = Self::T>
    where
        Self: 'a;
    type Iter<'a>: ExactSizeIterator<Item = Self::ItemType<'a>>
    where
        Self: 'a;

    /// An iterator over the points in this MultiPoint
    fn points(&self) -> Self::Iter<'_>;

    /// The number of points in this MultiPoint
    fn num_points(&self) -> usize;

    /// Access to a specified point in this MultiPoint
    /// Will return None if the provided index is out of bounds
    fn point(&self, i: usize) -> Option<Self::ItemType<'_>>;
}

impl<T: CoordNum> MultiPointTrait for MultiPoint<T> {
    type T = T;
    type ItemType<'a> = &'a Point<Self::T> where Self: 'a;
    type Iter<'a> = Iter<'a, Point<Self::T>> where T: 'a;

    fn points(&self) -> Self::Iter<'_> {
        self.0.iter()
    }

    fn num_points(&self) -> usize {
        self.0.len()
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i)
    }
}

impl<'a, T: CoordNum> MultiPointTrait for &'a MultiPoint<T> {
    type T = T;
    type ItemType<'b> = &'a Point<Self::T> where Self: 'b;
    type Iter<'b> = Iter<'a, Point<Self::T>> where Self: 'b;

    fn points(&self) -> Self::Iter<'_> {
        self.0.iter()
    }

    fn num_points(&self) -> usize {
        self.0.len()
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i)
    }
}
