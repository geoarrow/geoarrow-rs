use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::Translate;
use geoarrow::chunked_array::from_geoarrow_chunks;
use geoarrow::error::GeoArrowError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(signature = (geom, xoff=0.0, yoff=0.0))]
pub(crate) fn translate(
    py: Python,
    geom: AnyGeometryInput,
    xoff: f64,
    yoff: f64,
) -> PyGeoArrowResult<PyObject> {
    match geom {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().translate(&xoff.into(), &yoff.into())?;
            geometry_array_to_pyobject(py, out)
        }
        AnyGeometryInput::Chunked(chunked) => {
            let out = chunked
                .as_ref()
                .geometry_chunks()
                .iter()
                .map(|chunk| chunk.as_ref().translate(&xoff.into(), &yoff.into()))
                .collect::<Result<Vec<_>, GeoArrowError>>()?;
            let out_refs = out.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            chunked_geometry_array_to_pyobject(py, from_geoarrow_chunks(out_refs.as_slice())?)
        }
    }
}
