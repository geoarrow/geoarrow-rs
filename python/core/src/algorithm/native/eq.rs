#![allow(non_snake_case)]

use crate::array::*;
use crate::chunked_array::*;
use crate::scalar::*;
use crate::table::GeoTable;
use pyo3::prelude::*;

macro_rules! impl_eq {
    ($struct_name:ty) => {
        #[pymethods]
        impl $struct_name {
            /// Check for equality with other object.
            pub fn __eq__(&self, other: &$struct_name) -> bool {
                self.0 == other.0
            }
        }
    };
}

impl_eq!(Point);
impl_eq!(LineString);
impl_eq!(Polygon);
impl_eq!(MultiPoint);
impl_eq!(MultiLineString);
impl_eq!(MultiPolygon);
impl_eq!(Geometry);
impl_eq!(GeometryCollection);
impl_eq!(Rect);
impl_eq!(WKB);

impl_eq!(PointArray);
impl_eq!(LineStringArray);
impl_eq!(PolygonArray);
impl_eq!(MultiPointArray);
impl_eq!(MultiLineStringArray);
impl_eq!(MultiPolygonArray);
impl_eq!(MixedGeometryArray);
impl_eq!(GeometryCollectionArray);
impl_eq!(RectArray);

impl_eq!(BooleanArray);
impl_eq!(Float16Array);
impl_eq!(Float32Array);
impl_eq!(Float64Array);
impl_eq!(UInt8Array);
impl_eq!(UInt16Array);
impl_eq!(UInt32Array);
impl_eq!(UInt64Array);
impl_eq!(Int8Array);
impl_eq!(Int16Array);
impl_eq!(Int32Array);
impl_eq!(Int64Array);
impl_eq!(StringArray);
impl_eq!(LargeStringArray);

impl_eq!(ChunkedPointArray);
impl_eq!(ChunkedLineStringArray);
impl_eq!(ChunkedPolygonArray);
impl_eq!(ChunkedMultiPointArray);
impl_eq!(ChunkedMultiLineStringArray);
impl_eq!(ChunkedMultiPolygonArray);
impl_eq!(ChunkedMixedGeometryArray);
impl_eq!(ChunkedGeometryCollectionArray);
impl_eq!(ChunkedRectArray);

impl_eq!(ChunkedBooleanArray);
impl_eq!(ChunkedFloat16Array);
impl_eq!(ChunkedFloat32Array);
impl_eq!(ChunkedFloat64Array);
impl_eq!(ChunkedUInt8Array);
impl_eq!(ChunkedUInt16Array);
impl_eq!(ChunkedUInt32Array);
impl_eq!(ChunkedUInt64Array);
impl_eq!(ChunkedInt8Array);
impl_eq!(ChunkedInt16Array);
impl_eq!(ChunkedInt32Array);
impl_eq!(ChunkedInt64Array);
impl_eq!(ChunkedStringArray);
impl_eq!(ChunkedLargeStringArray);

impl_eq!(GeoTable);
