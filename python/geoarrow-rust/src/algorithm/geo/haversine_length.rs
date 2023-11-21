use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_haversine_length {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Determine the length of a geometry using the [haversine formula].
            ///
            /// [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula
            ///
            /// *Note*: this implementation uses a mean earth radius of 6371.088 km, based on the
            /// [recommendation of the IUGG](ftp://athena.fsv.cvut.cz/ZFG/grs80-Moritz.pdf)
            pub fn haversine_length(&self) -> Float64Array {
                use geoarrow::algorithm::geo::HaversineLength;
                HaversineLength::haversine_length(&self.0).into()
            }
        }
    };
}

impl_haversine_length!(PointArray);
impl_haversine_length!(MultiPointArray);
impl_haversine_length!(LineStringArray);
impl_haversine_length!(MultiLineStringArray);
