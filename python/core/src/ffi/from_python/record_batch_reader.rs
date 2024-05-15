use crate::ffi::from_python::utils::import_arrow_c_stream;
use crate::stream::PyRecordBatchReader;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyRecordBatchReader {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let stream = import_arrow_c_stream(ob)?;
        let stream_reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(Some(Box::new(stream_reader))))
    }
}
