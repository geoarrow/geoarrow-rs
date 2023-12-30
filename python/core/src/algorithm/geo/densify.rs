use crate::array::*;
use crate::chunked_array::*;
use pyo3::prelude::*;

macro_rules! impl_densify {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            pub fn densify(&self, max_distance: f64) -> Self {
                use geoarrow::algorithm::geo::Densify;
                Densify::densify(&self.0, max_distance).into()
            }
        }
    };
}

impl_densify!(LineStringArray);
impl_densify!(PolygonArray);
impl_densify!(MultiLineStringArray);
impl_densify!(MultiPolygonArray);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            pub fn densify(&self, max_distance: f64) -> Self {
                use geoarrow::algorithm::geo::Densify;
                Densify::densify(&self.0, max_distance).into()
            }
        }
    };
}

impl_vector!(ChunkedLineStringArray);
impl_vector!(ChunkedPolygonArray);
impl_vector!(ChunkedMultiLineStringArray);
impl_vector!(ChunkedMultiPolygonArray);
