use geo::CoordNum;

pub trait CoordTrait: Send + Sync {
    type T: CoordNum + Send + Sync;

    /// x component of this coord
    fn x(&self) -> Self::T;

    /// y component of this coord
    fn y(&self) -> Self::T;

    /// Returns a tuple that contains the x/horizontal & y/vertical component of the coord.
    fn x_y(&self) -> (Self::T, Self::T) {
        (self.x(), self.y())
    }
}
