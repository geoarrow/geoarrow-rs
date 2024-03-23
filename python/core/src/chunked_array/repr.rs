use crate::chunked_array::*;
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

impl_repr!(ChunkedPointArray);
impl_repr!(ChunkedLineStringArray);
impl_repr!(ChunkedPolygonArray);
impl_repr!(ChunkedMultiPointArray);
impl_repr!(ChunkedMultiLineStringArray);
impl_repr!(ChunkedMultiPolygonArray);
impl_repr!(ChunkedMixedGeometryArray);
impl_repr!(ChunkedGeometryCollectionArray);
impl_repr!(ChunkedWKBArray);
impl_repr!(ChunkedRectArray);
