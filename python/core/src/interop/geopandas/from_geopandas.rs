use std::sync::Arc;

use crate::array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_stream;
use crate::interop::shapely::from_shapely::from_shapely;
use crate::interop::util::import_pyarrow;
use crate::table::GeoTable;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow_array::RecordBatchReader;
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use pyo3::PyAny;

/// Create a GeoArrow Table from a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
///
/// ### Notes:
///
/// - Currently this will always generate a non-chunked GeoArrow array. This is partly because
///   [pyarrow.Table.from_pandas][pyarrow.Table.from_pandas] always creates a single batch.
/// - This requires `pyarrow` version 14 or later.
///
/// Args:
///   input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
///
/// Returns:
///     A GeoArrow Table
#[pyfunction]
pub fn from_geopandas(py: Python, input: &PyAny) -> PyGeoArrowResult<GeoTable> {
    GeoTable::from_geopandas(py.get_type::<GeoTable>(), py, input)
}

#[pymethods]
impl GeoTable {
    /// Create a GeoArrow Table from a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
    ///
    /// ### Notes:
    ///
    /// - Currently this will always generate a non-chunked GeoArrow array. This is partly because
    ///   [pyarrow.Table.from_pandas][pyarrow.Table.from_pandas] always creates a single batch.
    /// - This requires `pyarrow` version 14 or later.
    ///
    /// Args:
    ///   input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
    ///
    /// Returns:
    ///     A GeoArrow Table
    #[classmethod]
    fn from_geopandas(_cls: &PyType, py: Python, input: &PyAny) -> PyGeoArrowResult<Self> {
        // Imports and validation
        let pyarrow_mod = import_pyarrow(py)?;
        let geopandas_mod = py.import(intern!(py, "geopandas"))?;
        let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;
        if !input.is_instance(geodataframe_class)? {
            return Err(PyValueError::new_err("Expected GeoDataFrame input.").into());
        }

        // Convert main table to pyarrow table
        let geometry_column_name = input
            .getattr(intern!(py, "_geometry_column_name"))?
            .extract::<String>()?;
        let dataframe_column_names = input
            .getattr(intern!(py, "columns"))?
            .call_method0(intern!(py, "tolist"))?
            .extract::<Vec<String>>()?;
        let pyarrow_column_names = dataframe_column_names
            .into_iter()
            .filter(|name| name.as_str() != geometry_column_name.as_str())
            .collect::<Vec<_>>();

        let args = (input,);
        let kwargs = PyDict::new(py);
        kwargs.set_item("columns", pyarrow_column_names)?;
        let pyarrow_table = pyarrow_mod.getattr(intern!(py, "Table"))?.call_method(
            intern!(py, "from_pandas"),
            args,
            Some(kwargs),
        )?;

        // Move the pyarrow table into Rust
        let stream = import_arrow_c_stream(pyarrow_table)?;
        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }
        if batches.len() > 1 {
            return Err(PyValueError::new_err("Expected 1 batch from pyarrow table.").into());
        }

        // Convert GeoPandas geometry
        // Note: this is kinda a hack because from_ragged_array returns a _Python_ geoarrow class,
        // but I need to convert that back into a Rust object to make a ChunkedGeometryArray to
        // create the GeoTable
        let python_geometry_array = from_shapely(py, input.getattr(intern!(py, "geometry"))?)?;
        let chunked_geometry: Arc<dyn ChunkedGeometryArrayTrait> = if python_geometry_array
            .as_ref(py)
            .is_instance_of::<PointArray>()
        {
            let ga_arr = python_geometry_array.extract::<PointArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else if python_geometry_array
            .as_ref(py)
            .is_instance_of::<LineStringArray>()
        {
            let ga_arr = python_geometry_array.extract::<LineStringArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else if python_geometry_array
            .as_ref(py)
            .is_instance_of::<PolygonArray>()
        {
            let ga_arr = python_geometry_array.extract::<PolygonArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else if python_geometry_array
            .as_ref(py)
            .is_instance_of::<MultiPointArray>()
        {
            let ga_arr = python_geometry_array.extract::<MultiPointArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else if python_geometry_array
            .as_ref(py)
            .is_instance_of::<MultiLineStringArray>()
        {
            let ga_arr = python_geometry_array.extract::<MultiLineStringArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else if python_geometry_array
            .as_ref(py)
            .is_instance_of::<MultiPolygonArray>()
        {
            let ga_arr = python_geometry_array.extract::<MultiPolygonArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else if python_geometry_array
            .as_ref(py)
            .is_instance_of::<MixedGeometryArray>()
        {
            let ga_arr = python_geometry_array.extract::<MixedGeometryArray>(py)?;
            Arc::new(geoarrow::chunked_array::ChunkedGeometryArray::new(vec![
                ga_arr.0,
            ]))
        } else {
            unreachable!()
        };

        Ok(
            geoarrow::table::GeoTable::from_arrow_and_geometry(batches, schema, chunked_geometry)?
                .into(),
        )
    }
}
