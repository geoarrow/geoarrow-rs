from __future__ import annotations

from typing import overload

from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable

from ._array import GeoArray as GeoArray
from ._array_reader import GeoArrayReader as GeoArrayReader
from ._chunked_array import GeoChunkedArray as GeoChunkedArray
from ._constructors import CoordsInput as CoordsInput
from ._constructors import linestrings as linestrings
from ._constructors import multilinestrings as multilinestrings
from ._constructors import multipoints as multipoints
from ._constructors import multipolygons as multipolygons
from ._constructors import points as points
from ._constructors import polygons as polygons
from ._data_type import box as box
from ._data_type import geometry as geometry
from ._data_type import geometrycollection as geometrycollection
from ._data_type import GeoType as GeoType
from ._data_type import large_wkb as large_wkb
from ._data_type import large_wkt as large_wkt
from ._data_type import linestring as linestring
from ._data_type import multilinestring as multilinestring
from ._data_type import multipoint as multipoint
from ._data_type import multipolygon as multipolygon
from ._data_type import point as point
from ._data_type import polygon as polygon
from ._data_type import wkb as wkb
from ._data_type import wkb_view as wkb_view
from ._data_type import wkt as wkt
from ._data_type import wkt_view as wkt_view
from ._interop import from_shapely as from_shapely
from ._interop import from_wkb as from_wkb
from ._interop import from_wkt as from_wkt
from ._interop import to_wkb as to_wkb
from ._interop import to_wkt as to_wkt
from ._operations import get_type_id as get_type_id
from ._scalar import GeoScalar as GeoScalar

@overload
def geometry_col(
    input: ArrowArrayExportable,
    *,
    name: str | None = None,
) -> GeoArray: ...
@overload
def geometry_col(
    input: ArrowStreamExportable,
    *,
    name: str | None = None,
) -> GeoArrayReader: ...
def geometry_col(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    name: str | None = None,
) -> GeoArray | GeoArrayReader:
    """Access the geometry column of a Table or RecordBatch

    Args:
        input: The Arrow RecordBatch or Table to extract the geometry column from.

    Keyword Args:
        name: The name of the geometry column to extract. If not provided, an error will be produced if there are multiple columns with GeoArrow metadata.

    Returns:
        A geometry array or chunked array.
    """

# Interop

# def read_pyogrio(
#     path_or_buffer: Path | str | bytes,
#     /,
#     layer: int | str | None = None,
#     encoding: str | None = None,
#     columns: Sequence[str] | None = None,
#     read_geometry: bool = True,
#     skip_features: int = 0,
#     max_features: int | None = None,
#     where: str | None = None,
#     bbox: Tuple[float, float, float, float] | Sequence[float] | None = None,
#     mask: Any = None,
#     fids: Any = None,
#     sql: str | None = None,
#     sql_dialect: str | None = None,
#     return_fids=False,
#     batch_size=65536,
#     **kwargs: Any,
# ) -> Table:
#     """
#     Read from an OGR data source to an Arrow Table

#     Args:
#         path_or_buffer: A dataset path or URI, or raw buffer.
#         layer: If an integer is provided, it corresponds to the index of the layer
#             with the data source. If a string is provided, it must match the name
#             of the layer in the data source. Defaults to first layer in data source.
#         encoding: If present, will be used as the encoding for reading string values from
#             the data source, unless encoding can be inferred directly from the data
#             source.
#         columns: List of column names to import from the data source. Column names must
#             exactly match the names in the data source, and will be returned in
#             the order they occur in the data source. To avoid reading any columns,
#             pass an empty list-like.
#         read_geometry: If True, will read geometry into a GeoSeries. If False, a Pandas DataFrame
#             will be returned instead. Default: `True`.
#         skip_features: Number of features to skip from the beginning of the file before
#             returning features. If greater than available number of features, an
#             empty DataFrame will be returned. Using this parameter may incur
#             significant overhead if the driver does not support the capability to
#             randomly seek to a specific feature, because it will need to iterate
#             over all prior features.
#         max_features: Number of features to read from the file. Default: `None`.
#         where: Where clause to filter features in layer by attribute values. If the data source
#             natively supports SQL, its specific SQL dialect should be used (eg. SQLite and
#             GeoPackage: [`SQLITE`][SQLITE], PostgreSQL). If it doesn't, the [`OGRSQL
#             WHERE`][OGRSQL_WHERE] syntax should be used. Note that it is not possible to overrule
#             the SQL dialect, this is only possible when you use the `sql` parameter.

#             Examples: `"ISO_A3 = 'CAN'"`, `"POP_EST > 10000000 AND POP_EST < 100000000"`

#             [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
#             [OGRSQL_WHERE]: https://gdal.org/user/ogr_sql_dialect.html#where

#         bbox: If present, will be used to filter records whose geometry intersects this
#             box. This must be in the same CRS as the dataset. If GEOS is present
#             and used by GDAL, only geometries that intersect this bbox will be
#             returned; if GEOS is not available or not used by GDAL, all geometries
#             with bounding boxes that intersect this bbox will be returned.
#             Cannot be combined with `mask` keyword.
#         mask: Shapely geometry, optional (default: `None`)
#             If present, will be used to filter records whose geometry intersects
#             this geometry. This must be in the same CRS as the dataset. If GEOS is
#             present and used by GDAL, only geometries that intersect this geometry
#             will be returned; if GEOS is not available or not used by GDAL, all
#             geometries with bounding boxes that intersect the bounding box of this
#             geometry will be returned. Requires Shapely >= 2.0.
#             Cannot be combined with `bbox` keyword.
#         fids : array-like, optional (default: `None`)
#             Array of integer feature id (FID) values to select. Cannot be combined
#             with other keywords to select a subset (`skip_features`,
#             `max_features`, `where`, `bbox`, `mask`, or `sql`). Note that
#             the starting index is driver and file specific (e.g. typically 0 for
#             Shapefile and 1 for GeoPackage, but can still depend on the specific
#             file). The performance of reading a large number of features usings FIDs
#             is also driver specific.
#         sql: The SQL statement to execute. Look at the sql_dialect parameter for more
#             information on the syntax to use for the query. When combined with other
#             keywords like `columns`, `skip_features`, `max_features`,
#             `where`, `bbox`, or `mask`, those are applied after the SQL query.
#             Be aware that this can have an impact on performance, (e.g. filtering
#             with the `bbox` or `mask` keywords may not use spatial indexes).
#             Cannot be combined with the `layer` or `fids` keywords.
#         sql_dialect : str, optional (default: `None`)
#             The SQL dialect the SQL statement is written in. Possible values:

#             - **None**: if the data source natively supports SQL, its specific SQL dialect
#                 will be used by default (eg. SQLite and Geopackage: [`SQLITE`][SQLITE], PostgreSQL).
#                 If the data source doesn't natively support SQL, the [`OGRSQL`][OGRSQL] dialect is
#                 the default.
#             - [`'OGRSQL'`][OGRSQL]: can be used on any data source. Performance can suffer
#                 when used on data sources with native support for SQL.
#             - [`'SQLITE'`][SQLITE]: can be used on any data source. All [spatialite][spatialite]
#                 functions can be used. Performance can suffer on data sources with
#                 native support for SQL, except for Geopackage and SQLite as this is
#                 their native SQL dialect.

#             [OGRSQL]: https://gdal.org/user/ogr_sql_dialect.html#ogr-sql-dialect
#             [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
#             [spatialite]: https://www.gaia-gis.it/gaia-sins/spatialite-sql-latest.html

#     Keyword Args:
#         kwargs: Additional driver-specific dataset open options passed to OGR. Invalid
#             options will trigger a warning.

#     Returns:
#         Table
#     """

# def from_geopandas(input: gpd.GeoDataFrame) -> Table:
#     """
#     Create a GeoArrow Table from a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

#     ### Notes:

#     - Currently this will always generate a non-chunked GeoArrow array. This is partly because
#     [pyarrow.Table.from_pandas][pyarrow.Table.from_pandas] always creates a single batch.

#     Args:
#         input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

#     Returns:
#         A GeoArrow Table
#     """

# def from_shapely(input, *, crs: CRSInput | None = None) -> GeoArray:
#     """
#     Create a GeoArrow array from an array of Shapely geometries.

#     ### Notes:

#     - Currently this will always generate a non-chunked GeoArrow array.
#     - Under the hood, this will first call
#         [`shapely.to_ragged_array`][], falling back to [`shapely.to_wkb`][] if
#         necessary.

#         This is because `to_ragged_array` is the fastest approach but fails on
#         mixed-type geometries. It supports combining Multi-* geometries with
#         non-multi-geometries in the same array, so you can combine e.g. Point and
#         MultiPoint geometries in the same array, but `to_ragged_array` doesn't work if
#         you have Point and Polygon geometries in the same array.

#     Args:

#     input: Any array object accepted by Shapely, including numpy object arrays and
#     [`geopandas.GeoSeries`][geopandas.GeoSeries].

#     Returns:

#         A GeoArrow array
#     """

# def to_geopandas(input: ArrowStreamExportable) -> gpd.GeoDataFrame:
#     """
#     Convert a GeoArrow Table to a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

#     ### Notes:

#     - This is an alias to [GeoDataFrame.from_arrow][geopandas.GeoDataFrame.from_arrow].

#     Args:
#     input: A GeoArrow Table.

#     Returns:
#         the converted GeoDataFrame
#     """

# def to_shapely(
#     input: ArrowArrayExportable | ArrowStreamExportable,
# ) -> NDArray[np.object_]:
#     """
#     Convert a GeoArrow array to a numpy array of Shapely objects

#     Args:
#         input: input geometry array

#     Returns:
#         numpy array with Shapely objects
#     """
