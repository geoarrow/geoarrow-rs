use std::fmt;
use std::iter::Peekable;

use geozero::ToWkt;

use crate::array::*;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;

impl fmt::Display for PointArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PointArray([")?;

        // Skip the last one, as the last element won't add a comma
        for maybe_geom in self.iter().skip_last() {
            if let Some(geom) = maybe_geom {
                f.write_str(geom.to_wkt().unwrap().as_str())?;
                write!(f, ", ")?;
            } else {
                write!(f, "null, ")?;
            }
        }

        if let Some(geom) = self.get(self.len() - 1) {
            f.write_str(geom.to_wkt().unwrap().as_str())?;
        } else {
            write!(f, "null")?;
        }

        write!(f, "])")?;

        Ok(())
    }
}

// https://users.rust-lang.org/t/iterator-skip-last/45635/2
struct SkipLastIterator<I: Iterator>(Peekable<I>);
impl<I: Iterator> Iterator for SkipLastIterator<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next();
        self.0.peek().map(|_| item.unwrap())
    }
}

trait SkipLast: Iterator + Sized {
    fn skip_last(self) -> SkipLastIterator<Self> {
        SkipLastIterator(self.peekable())
    }
}

impl<I: Iterator> SkipLast for I {}
