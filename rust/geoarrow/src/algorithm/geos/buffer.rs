use crate::algorithm::geos::util::{try_unary_geometry, try_unary_polygon};
use crate::array::{GeometryArray, PointArray, PolygonArray};
use crate::error::Result;
use crate::NativeArray;
use geos::{BufferParams, Geom};

pub trait Buffer {
    type Output;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output;

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output;
}

impl Buffer for PointArray {
    type Output = Result<PolygonArray>;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output {
        try_unary_polygon(self, |g| g.buffer(width, quadsegs), self.dimension())
    }

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output {
        try_unary_polygon(
            self,
            |g| g.buffer_with_params(width, buffer_params),
            self.dimension(),
        )
    }
}

impl Buffer for GeometryArray {
    type Output = Result<GeometryArray>;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output {
        try_unary_geometry(self, |g| g.buffer(width, quadsegs))
    }

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output {
        try_unary_geometry(self, |g| g.buffer_with_params(width, buffer_params))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;

    #[test]
    fn point_buffer() {
        let arr = point_array();
        let buffered: PolygonArray = arr.buffer(1., 8).unwrap();
        dbg!(buffered);
    }
}
