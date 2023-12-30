use crate::array::*;
use crate::chunked_array::*;
use pyo3::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these
            /// `empty`.
            ///
            /// Types like `Point`, which have at least one coordinate by construction, can never
            /// be considered empty.
            pub fn is_empty(&self) -> BooleanArray {
                use geoarrow::algorithm::geo::HasDimensions;
                HasDimensions::is_empty(&self.0).into()
            }
        }
    };
}

impl_alg!(PointArray);
impl_alg!(LineStringArray);
impl_alg!(PolygonArray);
impl_alg!(MultiPointArray);
impl_alg!(MultiLineStringArray);
impl_alg!(MultiPolygonArray);
impl_alg!(MixedGeometryArray);
impl_alg!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these
            /// `empty`.
            ///
            /// Types like `Point`, which have at least one coordinate by construction, can never
            /// be considered empty.
            pub fn is_empty(&self) -> ChunkedBooleanArray {
                use geoarrow::algorithm::geo::HasDimensions;
                HasDimensions::is_empty(&self.0).unwrap().into()
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
