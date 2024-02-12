use crate::error::PyGeoArrowResult;
use crate::interop::util::import_pyarrow;
use crate::table::GeoTable;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyAny;

/// Read from an OGR data source to a GeoTable
///
/// Args:
///     path_or_buffer: A dataset path or URI, or raw buffer.
///     layer: If an integer is provided, it corresponds to the index of the layer
///         with the data source. If a string is provided, it must match the name
///         of the layer in the data source. Defaults to first layer in data source.
///     encoding: If present, will be used as the encoding for reading string values from
///         the data source, unless encoding can be inferred directly from the data
///         source.
///     columns: List of column names to import from the data source. Column names must
///         exactly match the names in the data source, and will be returned in
///         the order they occur in the data source. To avoid reading any columns,
///         pass an empty list-like.
///     read_geometry: If True, will read geometry into a GeoSeries. If False, a Pandas DataFrame
///         will be returned instead. Default: `True`.
///     skip_features: Number of features to skip from the beginning of the file before
///         returning features. If greater than available number of features, an
///         empty DataFrame will be returned. Using this parameter may incur
///         significant overhead if the driver does not support the capability to
///         randomly seek to a specific feature, because it will need to iterate
///         over all prior features.
///     max_features: Number of features to read from the file. Default: `None`.
///     where: Where clause to filter features in layer by attribute values. If the data source
///         natively supports SQL, its specific SQL dialect should be used (eg. SQLite and
///         GeoPackage: [`SQLITE`][SQLITE], PostgreSQL). If it doesn't, the [`OGRSQL
///         WHERE`][OGRSQL_WHERE] syntax should be used. Note that it is not possible to overrule
///         the SQL dialect, this is only possible when you use the `sql` parameter.
///
///         Examples: `"ISO_A3 = 'CAN'"`, `"POP_EST > 10000000 AND POP_EST < 100000000"`
///
///         [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
///         [OGRSQL_WHERE]: https://gdal.org/user/ogr_sql_dialect.html#where
///
///     bbox: If present, will be used to filter records whose geometry intersects this
///         box. This must be in the same CRS as the dataset. If GEOS is present
///         and used by GDAL, only geometries that intersect this bbox will be
///         returned; if GEOS is not available or not used by GDAL, all geometries
///         with bounding boxes that intersect this bbox will be returned.
///         Cannot be combined with `mask` keyword.
///     mask: Shapely geometry, optional (default: `None`)
///         If present, will be used to filter records whose geometry intersects
///         this geometry. This must be in the same CRS as the dataset. If GEOS is
///         present and used by GDAL, only geometries that intersect this geometry
///         will be returned; if GEOS is not available or not used by GDAL, all
///         geometries with bounding boxes that intersect the bounding box of this
///         geometry will be returned. Requires Shapely >= 2.0.
///         Cannot be combined with `bbox` keyword.
///     fids : array-like, optional (default: `None`)
///         Array of integer feature id (FID) values to select. Cannot be combined
///         with other keywords to select a subset (`skip_features`,
///         `max_features`, `where`, `bbox`, `mask`, or `sql`). Note that
///         the starting index is driver and file specific (e.g. typically 0 for
///         Shapefile and 1 for GeoPackage, but can still depend on the specific
///         file). The performance of reading a large number of features usings FIDs
///         is also driver specific.
///     sql: The SQL statement to execute. Look at the sql_dialect parameter for more
///         information on the syntax to use for the query. When combined with other
///         keywords like `columns`, `skip_features`, `max_features`,
///         `where`, `bbox`, or `mask`, those are applied after the SQL query.
///         Be aware that this can have an impact on performance, (e.g. filtering
///         with the `bbox` or `mask` keywords may not use spatial indexes).
///         Cannot be combined with the `layer` or `fids` keywords.
///     sql_dialect : str, optional (default: `None`)
///         The SQL dialect the SQL statement is written in. Possible values:
///
///           - **None**: if the data source natively supports SQL, its specific SQL dialect
///             will be used by default (eg. SQLite and Geopackage: [`SQLITE`][SQLITE], PostgreSQL).
///             If the data source doesn't natively support SQL, the [`OGRSQL`][OGRSQL] dialect is
///             the default.
///           - [`'OGRSQL'`][OGRSQL]: can be used on any data source. Performance can suffer
///             when used on data sources with native support for SQL.
///           - [`'SQLITE'`][SQLITE]: can be used on any data source. All [spatialite][spatialite]
///             functions can be used. Performance can suffer on data sources with
///             native support for SQL, except for Geopackage and SQLite as this is
///             their native SQL dialect.
///
///         [OGRSQL]: https://gdal.org/user/ogr_sql_dialect.html#ogr-sql-dialect
///         [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
///         [spatialite]: https://www.gaia-gis.it/gaia-sins/spatialite-sql-latest.html
///
///     **kwargs
///         Additional driver-specific dataset open options passed to OGR. Invalid
///         options will trigger a warning.
///
/// Returns:
///     Table
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (path_or_buffer, /, layer=None, encoding=None, columns=None, read_geometry=true, skip_features=0, max_features=None, r#where=None, bbox=None, mask=None, fids=None, sql=None, sql_dialect=None, return_fids=false, batch_size=65536, **kwargs))]
pub fn read_pyogrio(
    py: Python,
    path_or_buffer: &PyAny,
    layer: Option<&PyAny>,
    encoding: Option<&PyAny>,
    columns: Option<&PyAny>,
    read_geometry: bool,
    skip_features: usize,
    max_features: Option<&PyAny>,
    r#where: Option<&PyAny>,
    bbox: Option<&PyAny>,
    mask: Option<&PyAny>,
    fids: Option<&PyAny>,
    sql: Option<&PyAny>,
    sql_dialect: Option<&PyAny>,
    return_fids: bool,
    batch_size: usize,
    kwargs: Option<&PyDict>,
) -> PyGeoArrowResult<GeoTable> {
    // Imports and validation
    // Import pyarrow to validate it's >=14 and will have PyCapsule interface
    let _pyarrow_mod = import_pyarrow(py)?;
    let pyogrio_mod = py.import(intern!(py, "pyogrio"))?;

    let args = (path_or_buffer,);
    let our_kwargs = PyDict::new(py);
    our_kwargs.set_item("layer", layer)?;
    our_kwargs.set_item("encoding", encoding)?;
    our_kwargs.set_item("columns", columns)?;
    our_kwargs.set_item("read_geometry", read_geometry)?;
    // NOTE: We always read only 2D data for now.
    // Edit: ValueError: forcing 2D is not supported for Arrow
    // our_kwargs.set_item("force_2d", true)?;
    our_kwargs.set_item("skip_features", skip_features)?;
    our_kwargs.set_item("max_features", max_features)?;
    our_kwargs.set_item("where", r#where)?;
    our_kwargs.set_item("bbox", bbox)?;
    our_kwargs.set_item("mask", mask)?;
    our_kwargs.set_item("fids", fids)?;
    our_kwargs.set_item("sql", sql)?;
    our_kwargs.set_item("sql_dialect", sql_dialect)?;
    our_kwargs.set_item("return_fids", return_fids)?;
    our_kwargs.set_item("batch_size", batch_size)?;
    if let Some(kwargs) = kwargs {
        our_kwargs.update(kwargs.as_mapping())?;
    }

    let context_manager = pyogrio_mod.getattr(intern!(py, "raw"))?.call_method(
        intern!(py, "open_arrow"),
        args,
        Some(our_kwargs),
    )?;
    let (_meta, record_batch_reader) = context_manager
        .call_method0(intern!(py, "__enter__"))?
        .extract::<(PyObject, PyObject)>()?;

    let maybe_table =
        GeoTable::from_arrow(py.get_type::<GeoTable>(), record_batch_reader.as_ref(py));

    // If the eval threw an exception we'll pass it through to the context manager.
    // Otherwise, __exit__ is called with empty arguments (Python "None").
    // https://pyo3.rs/v0.20.2/python_from_rust.html#need-to-use-a-context-manager-from-rust
    match maybe_table {
        Ok(table) => {
            let none = py.None();
            context_manager.call_method1("__exit__", (&none, &none, &none))?;
            Ok(table)
        }
        Err(e) => {
            context_manager
                .call_method1("__exit__", (e.get_type(py), e.value(py), e.traceback(py)))?;
            Err(e.into())
        }
    }
}
