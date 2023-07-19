use crate::array::RectArray;
use crate::scalar::Rect;
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`RectArray`]
#[derive(Clone, Debug)]
pub struct RectArrayValuesIter<'a> {
    array: &'a RectArray,
    index: usize,
    end: usize,
}

impl<'a> RectArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a RectArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for RectArrayValuesIter<'a> {
    type Item = Rect<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(self.array.value(old))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for RectArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for RectArrayValuesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<'a> IntoIterator for &'a RectArray {
    type Item = Option<Rect<'a>>;
    type IntoIter = ZipValidity<Rect<'a>, RectArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> RectArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(&'a self) -> ZipValidity<Rect<'a>, RectArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(RectArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> RectArrayValuesIter<'a> {
        RectArrayValuesIter::new(self)
    }
}

// /// Iterator of values of a [`RectArray`]
// #[derive(Clone, Debug)]
// pub struct RectInteriorIterator<'a> {
//     geom: &'a Rect<'a>,
//     index: usize,
//     end: usize,
// }

// impl<'a> RectInteriorIterator<'a> {
//     #[inline]
//     pub fn new(geom: &'a Rect<'a>) -> Self {
//         Self {
//             geom,
//             index: 0,
//             end: geom.num_interiors(),
//         }
//     }
// }

// impl<'a> Iterator for RectInteriorIterator<'a> {
//     type Item = crate::scalar::LineString<'a>;

//     #[inline]
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.index == self.end {
//             return None;
//         }
//         let old = self.index;
//         self.index += 1;
//         self.geom.interior(old)
//     }

//     #[inline]
//     fn size_hint(&self) -> (usizeption<usize>) {
//         (self.end - self.index, Some(self.end - self.index))
//     }
// }

// impl<'a> ExactSizeIterator for RectInteriorIterator<'a> {
//     // fn len(&self) -> usize {
//     //     self.end
//     // }
// }

// unsafe impl<'a> TrustedLen for RectInteriorIterator<'a> {}

// impl<'a> DoubleEndedIterator for RectInteriorIterator<'a> {
//     #[inline]
//     fn next_back(&mut self) -> Option<Self::Item> {
//         if self.index == self.end {
//             None
//         } else {
//             self.end -= 1;
//             self.geom.interior(self.end)
//         }
//     }
// }

// impl<'a> IntoIterator for &'a Rect<'a> {
//     type Item = LineString<'a>;
//     type IntoIter = RectInteriorIterator<'a>;

//     fn into_iter(self) -> Self::IntoIter {
//         self.iter()
//     }
// }

// impl<'a> Rect<'a> {
//     /// Returns an iterator of `Point`
//     pub fn iter(&'a self) -> RectInteriorIterator<'a> {
//         RectInteriorIterator::new(self)
//     }
// }
