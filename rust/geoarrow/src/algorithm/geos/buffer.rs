use crate::algorithm::geos::util::try_unary_polygon;
use crate::array::{PointArray, PolygonArray};
use crate::error::Result;
use geos::{BufferParams, Geom};

pub trait Buffer {
    type Output;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output;

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output;
}

impl<const D: usize> Buffer for PointArray<D> {
    type Output = Result<PolygonArray<D>>;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output {
        try_unary_polygon(self, |g| g.buffer(width, quadsegs))
    }

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output {
        try_unary_polygon(self, |g| g.buffer_with_params(width, buffer_params))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;

    #[test]
    fn point_buffer() {
        let arr = point_array();
        let buffered: PolygonArray<2> = arr.buffer(1., 8).unwrap();
        dbg!(buffered);
    }
}
