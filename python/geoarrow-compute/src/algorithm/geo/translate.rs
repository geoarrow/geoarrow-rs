use crate::ffi::from_python::AnyNativeInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::Translate;
use geoarrow::chunked_array::ChunkedNativeArrayDyn;
use geoarrow::error::GeoArrowError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(signature = (geom, xoff=0.0, yoff=0.0))]
pub(crate) fn translate(
    py: Python,
    geom: AnyNativeInput,
    xoff: f64,
    yoff: f64,
) -> PyGeoArrowResult<PyObject> {
    match geom {
        AnyNativeInput::Array(arr) => {
            let out = arr.as_ref().translate(&xoff.into(), &yoff.into())?;
            return_geometry_array(py, out)
        }
        AnyNativeInput::Chunked(chunked) => {
            let out = chunked
                .as_ref()
                .geometry_chunks()
                .iter()
                .map(|chunk| chunk.as_ref().translate(&xoff.into(), &yoff.into()))
                .collect::<Result<Vec<_>, GeoArrowError>>()?;
            let out_refs = out.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            return_chunked_geometry_array(
                py,
                ChunkedNativeArrayDyn::from_geoarrow_chunks(out_refs.as_slice())?.into_inner(),
            )
        }
    }
}
