use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
use pyo3::prelude::*;

macro_rules! impl_translate {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Translate a Geometry along its axes by the given offsets
            pub fn translate(
                &self,
                x_offset: BroadcastableFloat,
                y_offset: BroadcastableFloat,
            ) -> Self {
                use geoarrow::algorithm::geo::Translate;
                Translate::translate(&self.0, x_offset.0, y_offset.0).into()
            }
        }
    };
}

impl_translate!(PointArray);
impl_translate!(LineStringArray);
impl_translate!(PolygonArray);
impl_translate!(MultiPointArray);
impl_translate!(MultiLineStringArray);
impl_translate!(MultiPolygonArray);
