use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_vincenty_length {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Determine the length of a geometry using [Vincenty’s formulae].
            ///
            /// [Vincenty’s formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
            pub fn vincenty_length(&self) -> PyResult<Float64Array> {
                use geoarrow::algorithm::geo::VincentyLength;
                let result = VincentyLength::vincenty_length(&self.0).unwrap();
                Ok(result.into())
            }
        }
    };
}

impl_vincenty_length!(PointArray);
impl_vincenty_length!(MultiPointArray);
impl_vincenty_length!(LineStringArray);
impl_vincenty_length!(MultiLineStringArray);
