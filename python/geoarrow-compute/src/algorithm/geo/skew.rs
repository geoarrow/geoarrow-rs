use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::Skew;
use geoarrow::chunked_array::ChunkedNativeArrayDyn;
use geoarrow::error::GeoArrowError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(signature = (geom, xs=0.0, ys=0.0))]
pub fn skew(py: Python, geom: AnyGeometryInput, xs: f64, ys: f64) -> PyGeoArrowResult<PyObject> {
    match geom {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().skew_xy(&xs.into(), &ys.into())?;
            return_geometry_array(py, out)
        }
        AnyGeometryInput::Chunked(chunked) => {
            let out = chunked
                .as_ref()
                .geometry_chunks()
                .iter()
                .map(|chunk| chunk.as_ref().skew_xy(&xs.into(), &ys.into()))
                .collect::<Result<Vec<_>, GeoArrowError>>()?;
            let out_refs = out.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            return_chunked_geometry_array(
                py,
                ChunkedNativeArrayDyn::from_geoarrow_chunks(out_refs.as_slice())?.into_inner(),
            )
        }
    }
}
