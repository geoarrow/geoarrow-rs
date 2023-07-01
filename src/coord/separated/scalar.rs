use arrow2::buffer::Buffer;

pub struct SeparatedCoord<'a> {
    x: &'a Buffer<f64>,
    y: &'a Buffer<f64>,
    i: usize,
}

impl From<SeparatedCoord<'_>> for geo::Coord {
    fn from(value: SeparatedCoord) -> Self {
        geo::Coord {
            x: *value.x.get(value.i).unwrap(),
            y: *value.y.get(value.i).unwrap(),
        }
    }
}
