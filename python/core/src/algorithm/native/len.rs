use crate::array::*;
use crate::chunked_array::*;
use crate::table::GeoTable;
use arrow_array::Array;
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

impl_len!(BooleanArray);
impl_len!(Float16Array);
impl_len!(Float32Array);
impl_len!(Float64Array);
impl_len!(UInt8Array);
impl_len!(UInt16Array);
impl_len!(UInt32Array);
impl_len!(UInt64Array);
impl_len!(Int8Array);
impl_len!(Int16Array);
impl_len!(Int32Array);
impl_len!(Int64Array);
impl_len!(StringArray);
impl_len!(LargeStringArray);

impl_len!(ChunkedPointArray);
impl_len!(ChunkedLineStringArray);
impl_len!(ChunkedPolygonArray);
impl_len!(ChunkedMultiPointArray);
impl_len!(ChunkedMultiLineStringArray);
impl_len!(ChunkedMultiPolygonArray);
impl_len!(ChunkedMixedGeometryArray);
impl_len!(ChunkedGeometryCollectionArray);
impl_len!(ChunkedRectArray);

impl_len!(ChunkedBooleanArray);
impl_len!(ChunkedFloat16Array);
impl_len!(ChunkedFloat32Array);
impl_len!(ChunkedFloat64Array);
impl_len!(ChunkedUInt8Array);
impl_len!(ChunkedUInt16Array);
impl_len!(ChunkedUInt32Array);
impl_len!(ChunkedUInt64Array);
impl_len!(ChunkedInt8Array);
impl_len!(ChunkedInt16Array);
impl_len!(ChunkedInt32Array);
impl_len!(ChunkedInt64Array);
impl_len!(ChunkedStringArray);
impl_len!(ChunkedLargeStringArray);

impl_len!(GeoTable);
