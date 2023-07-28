use crate::error::GeoArrowError;
use crate::geo_traits::PointTrait;
use crate::scalar::Point;
use geos::{CoordDimensions, CoordSeq};

impl<'b> TryFrom<Point<'_>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: Point<'_>) -> Result<geos::Geometry<'b>, Self::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b> TryFrom<&'a Point<'_>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a Point<'_>) -> Result<geos::Geometry<'b>, Self::Error> {
        let mut coord_seq =
            CoordSeq::new(1, CoordDimensions::TwoD).expect("failed to create CoordSeq");
        coord_seq.set_x(0, value.x())?;
        coord_seq.set_y(0, value.y())?;

        Ok(geos::Geometry::create_point(coord_seq)?)
    }
}
