use crate::ffi::from_python::utils::import_arrow_c_stream;
use crate::table::GeoTable;
use arrow::ffi_stream::ArrowArrayStreamReader as ArrowRecordBatchStreamReader;
use arrow_array::RecordBatchReader;
use geoarrow::algorithm::native::DowncastTable;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for GeoTable {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let stream = import_arrow_c_stream(ob)?;
        let stream_reader = ArrowRecordBatchStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }

        let table = geoarrow::table::Table::try_new(schema, batches)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let table = table
            .downcast(true)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        // TODO: restore validation that all arrays have i32 offsets

        // if let Ok(data_type) = table.geometry_data_type() {
        //     match data_type {
        //         GeoDataType::LargeLineString(_)
        //         | GeoDataType::LargePolygon(_)
        //         | GeoDataType::LargeMultiPoint(_)
        //         | GeoDataType::LargeMultiLineString(_)
        //         | GeoDataType::LargeMultiPolygon(_)
        //         | GeoDataType::LargeMixed(_)
        //         | GeoDataType::LargeWKB
        //         | GeoDataType::LargeGeometryCollection(_) => return Err(PyValueError::new_err(
        //             "Unable to downcast from large to small offsets. Are your offsets 2^31 long?",
        //         )),
        //         _ => (),
        //     }
        // }

        Ok(table.into())
    }
}
