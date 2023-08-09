use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_center {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Compute the center of geometries
            ///
            /// This first computes the axis-aligned bounding rectangle, then takes the center of
            /// that box
            pub fn center(&self) -> PointArray {
                use geoarrow::algorithm::geo::Center;
                PointArray(Center::center(&self.0))
            }
        }
    };
}

impl_center!(PointArray);
impl_center!(LineStringArray);
impl_center!(PolygonArray);
impl_center!(MultiPointArray);
impl_center!(MultiLineStringArray);
impl_center!(MultiPolygonArray);
