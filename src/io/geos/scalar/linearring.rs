use crate::error::Result;
use geos::{Geom, GeometryTypes};

pub struct GEOSConstLinearRing<'a, 'b>(pub(crate) geos::ConstGeometry<'a, 'b>);

impl<'a, 'b> GEOSConstLinearRing<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::LinearRing));

        Ok(Self(geom))
    }

    pub fn num_coords(&self) -> usize {
        self.0.get_num_coordinates().unwrap()
    }
}
