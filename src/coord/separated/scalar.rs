use arrow2::buffer::Buffer;

pub struct SeparatedCoord<'a> {
    pub x: &'a Buffer<f64>,
    pub y: &'a Buffer<f64>,
    pub i: usize,
}

impl From<SeparatedCoord<'_>> for geo::Coord {
    fn from(value: SeparatedCoord) -> Self {
        geo::Coord {
            x: *value.x.get(value.i).unwrap(),
            y: *value.y.get(value.i).unwrap(),
        }
    }
}

impl From<SeparatedCoord<'_>> for geo::Point {
    fn from(value: SeparatedCoord<'_>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}
