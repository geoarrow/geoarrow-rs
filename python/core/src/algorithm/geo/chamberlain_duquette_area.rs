use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_unsigned_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0).into()
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_signed_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0).into()
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
// impl_alg!(GeometryArray);
