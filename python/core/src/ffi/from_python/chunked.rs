use crate::chunked_array::*;
use geoarrow::chunked_array::from_arrow_chunks;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};
use pyo3_arrow::input::AnyArray;

impl<'a> FromPyObject<'a> for PyChunkedGeometryArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let chunked_array = ob.extract::<AnyArray>()?.into_chunked_array()?;
        let (chunks, field) = chunked_array.into_inner();
        let array_refs = chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>();

        Ok(PyChunkedGeometryArray(
            from_arrow_chunks(array_refs.as_slice(), &field).unwrap(),
        ))
    }
}
