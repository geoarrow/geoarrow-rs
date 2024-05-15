use arrow_array::RecordBatchReader as _RecordBatchReader;
use geoarrow::error::GeoArrowError;
use pyo3::prelude::*;

use crate::error::PyGeoArrowResult;

/// A wrapper around an [arrow_array::RecordBatchReader]
#[pyclass(
    module = "geoarrow.rust.core._rust",
    name = "RecordBatchReader",
    subclass
)]
pub struct PyRecordBatchReader(pub(crate) Option<Box<dyn _RecordBatchReader + Send>>);

impl PyRecordBatchReader {
    pub fn into_reader(mut self) -> PyGeoArrowResult<Box<dyn _RecordBatchReader + Send>> {
        let stream = self.0.take().ok_or(GeoArrowError::General(
            "Cannot write from closed stream.".to_string(),
        ))?;
        Ok(stream)
    }
}
