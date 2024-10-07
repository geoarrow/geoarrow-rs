use crate::trait_::NativeGeometryAccessor;

#[derive(Debug)]
pub enum BroadcastableGeoGeometry<'a> {
    Scalar(&'a geo::Geometry),
    Array(&'a dyn NativeGeometryAccessor<'a, 2>),
}
