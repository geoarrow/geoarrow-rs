use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
use pyo3::prelude::*;

macro_rules! impl_densify {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            pub fn densify(&self, max_distance: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Densify;
                Densify::densify(&self.0, max_distance.0).into()
            }
        }
    };
}

impl_densify!(LineStringArray);
impl_densify!(PolygonArray);
impl_densify!(MultiLineStringArray);
impl_densify!(MultiPolygonArray);
