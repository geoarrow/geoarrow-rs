use std::sync::Arc;
use crate::trait_::GeometryScalarTrait;

pub type GeometryScalarRef<'a> = Arc<dyn GeometryScalarTrait<'a, ScalarGeo=geo::Geometry>>;

impl<'a> GeometryScalarTrait<'a> for GeometryScalarRef<'a> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.to_geo()
    }
}