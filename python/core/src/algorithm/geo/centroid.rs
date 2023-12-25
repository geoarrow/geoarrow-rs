use crate::array::*;
use crate::ffi::from_python::import_arrow_c_array;
use geoarrow::algorithm::geo::Centroid;
use geoarrow::array::from_arrow_array;
use pyo3::prelude::*;

#[pyfunction]
pub fn centroid(ob: &PyAny) -> PyResult<PointArray> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = from_arrow_array(&array, &field).unwrap();
    // TODO: fix error handling
    Ok(array.as_ref().centroid().unwrap().into())
}

macro_rules! impl_centroid {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the centroid.
            ///
            /// The centroid is the arithmetic mean position of all points in the shape.
            /// Informally, it is the point at which a cutout of the shape could be perfectly
            /// balanced on the tip of a pin.
            ///
            /// The geometric centroid of a convex object always lies in the object.
            /// A non-convex object might have a centroid that _is outside the object itself_.
            pub fn centroid(&self) -> PointArray {
                use geoarrow::algorithm::geo::Centroid;
                PointArray(Centroid::centroid(&self.0))
            }
        }
    };
}

impl_centroid!(PointArray);
impl_centroid!(LineStringArray);
impl_centroid!(PolygonArray);
impl_centroid!(MultiPointArray);
impl_centroid!(MultiLineStringArray);
impl_centroid!(MultiPolygonArray);
impl_centroid!(MixedGeometryArray);
impl_centroid!(GeometryCollectionArray);
