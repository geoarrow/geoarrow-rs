use arrow_array::RecordBatchReader as _RecordBatchReader;
use pyo3::prelude::*;

/// A wrapper around an [arrow_array::RecordBatchReader]
#[pyclass(
    module = "geoarrow.rust.core._rust",
    name = "RecordBatchReader",
    subclass
)]
pub struct PyRecordBatchReader(pub(crate) Option<Box<dyn _RecordBatchReader + Send>>);
