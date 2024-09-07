use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::Scale;
use geoarrow::chunked_array::from_geoarrow_chunks;
use geoarrow::error::GeoArrowError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(signature = (geom, xfact=1.0, yfact=1.0))]
pub fn scale(
    py: Python,
    geom: AnyGeometryInput,
    xfact: f64,
    yfact: f64,
) -> PyGeoArrowResult<PyObject> {
    match geom {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().scale_xy(&xfact.into(), &yfact.into())?;
            return_geometry_array(py, out)
        }
        AnyGeometryInput::Chunked(chunked) => {
            let out = chunked
                .as_ref()
                .geometry_chunks()
                .iter()
                .map(|chunk| chunk.as_ref().scale_xy(&xfact.into(), &yfact.into()))
                .collect::<Result<Vec<_>, GeoArrowError>>()?;
            let out_refs = out.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            return_chunked_geometry_array(py, from_geoarrow_chunks(out_refs.as_slice())?)
        }
    }
}
