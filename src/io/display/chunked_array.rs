use std::fmt;

use arrow_array::OffsetSizeTrait;

use crate::chunked_array::*;
use crate::io::display::array::{write_indented_ellipsis, WriteArray};

impl fmt::Display for ChunkedPointArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ChunkedPointArray")?;
        writeln!(f, "([")?;

        if self.num_chunks() > 6 {
            for chunk in self.chunks().iter().take(3) {
                chunk.write(f, 4)?;
                writeln!(f, ",")?;
            }
            write_indented_ellipsis(f, 4)?;
            for chunk in self.chunks().iter().rev().take(3).rev() {
                chunk.write(f, 4)?;
                writeln!(f, ",")?;
            }
        } else {
            for chunk in self.chunks().iter() {
                chunk.write(f, 4)?;
                writeln!(f, ",")?;
            }
        }
        write!(f, "])")?;
        Ok(())
    }
}

impl fmt::Display for ChunkedRectArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ChunkedRectArray")?;
        writeln!(f, "([")?;

        if self.num_chunks() > 6 {
            for chunk in self.chunks().iter().take(3) {
                chunk.write(f, 4)?;
                writeln!(f, ",")?;
            }
            write_indented_ellipsis(f, 4)?;
            for chunk in self.chunks().iter().rev().take(3).rev() {
                chunk.write(f, 4)?;
                writeln!(f, ",")?;
            }
        } else {
            for chunk in self.chunks().iter() {
                chunk.write(f, 4)?;
                writeln!(f, ",")?;
            }
        }
        write!(f, "])")?;
        Ok(())
    }
}

macro_rules! impl_fmt_generic {
    ($struct_name:ty, $str_literal:tt) => {
        impl<O: OffsetSizeTrait> fmt::Display for $struct_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(O::PREFIX)?;
                f.write_str($str_literal)?;
                writeln!(f, "([")?;

                if self.num_chunks() > 6 {
                    for chunk in self.chunks().iter().take(3) {
                        chunk.write(f, 4)?;
                        writeln!(f, ",")?;
                    }
                    write_indented_ellipsis(f, 4)?;
                    for chunk in self.chunks().iter().rev().take(3).rev() {
                        chunk.write(f, 4)?;
                        writeln!(f, ",")?;
                    }
                } else {
                    for chunk in self.chunks().iter() {
                        chunk.write(f, 4)?;
                        writeln!(f, ",")?;
                    }
                }
                write!(f, "])")?;
                Ok(())
            }
        }
    };
}

impl_fmt_generic!(ChunkedLineStringArray<O>, "ChunkedLineStringArray");
impl_fmt_generic!(ChunkedPolygonArray<O>, "ChunkedPolygonArray");
impl_fmt_generic!(ChunkedMultiPointArray<O>, "ChunkedMultiPointArray");
impl_fmt_generic!(
    ChunkedMultiLineStringArray<O>,
    "ChunkedMultiLineStringArray"
);
impl_fmt_generic!(ChunkedMultiPolygonArray<O>, "ChunkedMultiPolygonArray");
impl_fmt_generic!(ChunkedMixedGeometryArray<O>, "ChunkedMixedGeometryArray");
impl_fmt_generic!(
    ChunkedGeometryCollectionArray<O>,
    "ChunkedGeometryCollectionArray"
);
impl_fmt_generic!(ChunkedWKBArray<O>, "ChunkedWKBArray");

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point;

    #[test]
    fn test_display_chunked_point_array() {
        let point_array = point::point_array();
        let _chunked = ChunkedPointArray::new(vec![point_array; 3]);
        // println!("{}", chunked);
    }

    #[test]
    fn test_display_chunked_point_array_large() {
        let point_array = point::point_array();
        let _chunked = ChunkedPointArray::new(vec![point_array; 7]);
        // println!("{}", chunked);
    }
}
