use std::sync::Arc;

use crate::ffi::from_python::utils::import_arrow_c_array;
use crate::record_batch::RecordBatch;
use arrow::array::AsArray;
use arrow::datatypes::{DataType, SchemaBuilder};
use arrow_array::Array;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for RecordBatch {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let (array, field) = import_arrow_c_array(ob)?;
        match field.data_type() {
            DataType::Struct(fields) => {
                let struct_array = array.as_struct();
                let schema = SchemaBuilder::from(fields)
                    .finish()
                    .with_metadata(field.metadata().clone());
                assert_eq!(
                    struct_array.null_count(),
                    0,
                    "Cannot convert nullable StructArray to RecordBatch"
                );

                let columns = struct_array.columns().to_vec();
                let batch = arrow_array::RecordBatch::try_new(Arc::new(schema), columns)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                Ok(Self(batch))
            }
            dt => Err(PyValueError::new_err(format!(
                "Unexpected data type {}",
                dt
            ))),
        }
    }
}
