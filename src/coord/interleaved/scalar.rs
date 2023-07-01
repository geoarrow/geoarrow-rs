use arrow2::buffer::Buffer;

pub struct InterleavedCoord<'a> {
    coords: &'a Buffer<f64>,
    i: usize,
}
