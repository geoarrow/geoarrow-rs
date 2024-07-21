use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use geoarrow::array::AsChunkedGeometryArray;
use geoarrow::datatypes::GeoDataType;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_arrow::PyTable;

/// Convert a GeoArrow Table to a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
///
/// ### Notes:
///
/// - This requires [`pyarrow`][pyarrow] version 14 or later.
///
/// Args:
///   input: A GeoArrow Table.
///
/// Returns:
///     the converted GeoDataFrame
#[pyfunction]
pub fn to_geopandas(py: Python, input: PyTable) -> PyGeoArrowResult<PyObject> {
    // Imports and validation
    let geopandas_mod = py.import_bound(intern!(py, "geopandas"))?;
    let pandas_mod = py.import_bound(intern!(py, "pandas"))?;
    let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;

    let (batches, schema) = input.into_inner();
    let rust_table = geoarrow::table::Table::try_new(schema.clone(), batches.clone())?;

    let pyarrow_table = PyTable::new(schema, batches).to_pyarrow(py)?;

    let geometry_column_index = rust_table.default_geometry_column_idx()?;
    let pyarrow_table =
        pyarrow_table.call_method1(py, intern!(py, "remove_column"), (geometry_column_index,))?;

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item(
        "types_mapper",
        pandas_mod.getattr(intern!(py, "ArrowDtype"))?,
    )?;
    let pandas_df =
        pyarrow_table.call_method_bound(py, intern!(py, "to_pandas"), (), Some(&kwargs))?;

    let geometry = rust_table.geometry_column(Some(geometry_column_index))?;
    let shapely_geometry = match geometry.data_type() {
        GeoDataType::Point(_) => ChunkedPointArray(geometry.as_ref().as_point_2d().clone())
            .to_shapely(py)?
            .to_object(py),
        GeoDataType::LineString(_) => {
            ChunkedLineStringArray(geometry.as_ref().as_line_string_2d().clone())
                .to_shapely(py)?
                .to_object(py)
        }
        GeoDataType::Polygon(_) => ChunkedPolygonArray(geometry.as_ref().as_polygon_2d().clone())
            .to_shapely(py)?
            .to_object(py),
        GeoDataType::MultiPoint(_) => {
            ChunkedMultiPointArray(geometry.as_ref().as_multi_point_2d().clone())
                .to_shapely(py)?
                .to_object(py)
        }
        GeoDataType::MultiLineString(_) => {
            ChunkedMultiLineStringArray(geometry.as_ref().as_multi_line_string_2d().clone())
                .to_shapely(py)?
                .to_object(py)
        }
        GeoDataType::MultiPolygon(_) => {
            ChunkedMultiPolygonArray(geometry.as_ref().as_multi_polygon_2d().clone())
                .to_shapely(py)?
                .to_object(py)
        }
        GeoDataType::Mixed(_) => ChunkedMixedGeometryArray(geometry.as_ref().as_mixed_2d().clone())
            .to_shapely(py)?
            .to_object(py),
        GeoDataType::GeometryCollection(_) => {
            ChunkedGeometryCollectionArray(geometry.as_ref().as_geometry_collection_2d().clone())
                .to_shapely(py)?
                .to_object(py)
        }
        GeoDataType::WKB => ChunkedWKBArray(geometry.as_ref().as_wkb().clone())
            .to_shapely(py)?
            .to_object(py),
        t => {
            return Err(PyValueError::new_err(format!("unexpected type {:?}", t)).into());
        }
    };

    let args = (pandas_df,);
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("geometry", shapely_geometry)?;
    Ok(geodataframe_class.call(args, Some(&kwargs))?.to_object(py))
}
