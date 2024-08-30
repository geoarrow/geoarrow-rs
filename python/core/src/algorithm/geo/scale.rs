use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::Scale;
use geoarrow::chunked_array::from_geoarrow_chunks;
use geoarrow::error::GeoArrowError;
use pyo3::prelude::*;

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
            geometry_array_to_pyobject(py, out)
        }
        AnyGeometryInput::Chunked(chunked) => {
            let out = chunked
                .as_ref()
                .geometry_chunks()
                .iter()
                .map(|chunk| chunk.as_ref().scale_xy(&xfact.into(), &yfact.into()))
                .collect::<Result<Vec<_>, GeoArrowError>>()?;
            let out_refs = out.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            chunked_geometry_array_to_pyobject(py, from_geoarrow_chunks(out_refs.as_slice())?)
        }
    }
}
