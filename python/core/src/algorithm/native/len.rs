use crate::array::*;
use crate::chunked_array::*;
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

macro_rules! impl_len {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// The number of rows
            pub fn __len__(&self) -> usize {
                self.0.len()
            }
        }
    };
}

impl_len!(PointArray);
impl_len!(LineStringArray);
impl_len!(PolygonArray);
impl_len!(MultiPointArray);
impl_len!(MultiLineStringArray);
impl_len!(MultiPolygonArray);
impl_len!(MixedGeometryArray);
impl_len!(GeometryCollectionArray);
impl_len!(RectArray);

impl_len!(ChunkedPointArray);
impl_len!(ChunkedLineStringArray);
impl_len!(ChunkedPolygonArray);
impl_len!(ChunkedMultiPointArray);
impl_len!(ChunkedMultiLineStringArray);
impl_len!(ChunkedMultiPolygonArray);
impl_len!(ChunkedMixedGeometryArray);
impl_len!(ChunkedGeometryCollectionArray);
impl_len!(ChunkedRectArray);
