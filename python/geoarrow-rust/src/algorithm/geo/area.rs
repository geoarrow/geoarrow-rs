use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            pub fn area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::Area;
                Area::unsigned_area(&self.0).into()
            }

            /// Signed planar area of a geometry.
            pub fn signed_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::Area;
                Area::signed_area(&self.0).into()
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);
// impl_area!(GeometryArray);
