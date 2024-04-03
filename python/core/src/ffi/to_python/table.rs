use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchIterator;

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CString;

#[pymethods]
impl GeoTable {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this array
    /// into a pyarrow table, without copying memory.
    fn __arrow_c_stream__(
        &self,
        _requested_schema: Option<PyObject>,
    ) -> PyGeoArrowResult<PyObject> {
        let (schema, batches) = self.0.clone().into_inner();
        let record_batch_reader = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        ));
        let ffi_stream = FFI_ArrowArrayStream::new(record_batch_reader);

        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

        Python::with_gil(|py| {
            let stream_capsule = PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?;
            Ok(stream_capsule.to_object(py))
        })
    }
}
