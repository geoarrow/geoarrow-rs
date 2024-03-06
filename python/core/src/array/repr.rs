use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_repr {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Text representation
            pub fn __repr__(&self) -> String {
                self.0.to_string()
            }
        }
    };
}

impl_repr!(PointArray);
impl_repr!(LineStringArray);
impl_repr!(PolygonArray);
impl_repr!(MultiPointArray);
impl_repr!(MultiLineStringArray);
impl_repr!(MultiPolygonArray);
impl_repr!(MixedGeometryArray);
impl_repr!(GeometryCollectionArray);
impl_repr!(WKBArray);
impl_repr!(RectArray);
