use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the length of a Line
            pub fn euclidean_length(&self) -> Float64Array {
                use geoarrow::algorithm::geo::EuclideanLength;
                EuclideanLength::euclidean_length(&self.0).into()
            }
        }
    };
}

impl_euclidean_length!(PointArray);
impl_euclidean_length!(MultiPointArray);
impl_euclidean_length!(LineStringArray);
impl_euclidean_length!(MultiLineStringArray);
