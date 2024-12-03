from __future__ import annotations

from pathlib import Path
from typing import (
    List,
    Literal,
    Self,
    Sequence,
    Tuple,
    overload,
)

from arro3.core import Array, ChunkedArray, Table
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

try:
    import numpy as np
    from numpy.typing import NDArray
except ImportError:
    pass

try:
    import geopandas as gpd
except ImportError:
    pass

from geoarrow.rust.core._constructors import linestrings as linestrings
from geoarrow.rust.core._constructors import multilinestrings as multilinestrings
from geoarrow.rust.core._constructors import multipoints as multipoints
from geoarrow.rust.core._constructors import multipolygons as multipolygons
from geoarrow.rust.core._constructors import points as points
from geoarrow.rust.core._constructors import polygons as polygons
from geoarrow.rust.core.types import CRSInput

from .enums import CoordType, Dimension
from .types import CoordTypeT, DimensionT

class Geometry:
    """
    An immutable geometry scalar using GeoArrow's in-memory representation.

    **Note**: for best performance, do as many operations as possible on arrays or chunked
    arrays instead of scalars.
    """
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this
        array into a pyarrow array, without copying memory.
        """
    def __eq__(self, other: object) -> bool: ...
    @property
    def __geo_interface__(self) -> dict:
        """Implements the "geo interface protocol".

        See <https://gist.github.com/sgillies/2217756>
        """
    def __repr__(self) -> str:
        """Text representation."""
    def _repr_svg_(self) -> str:
        """Render as SVG in IPython/Jupyter."""

class NativeArray:
    """An immutable array of geometries using GeoArrow's in-memory representation."""
    def __init__(self, data: ArrowArrayExportable) -> None: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this
        array into a pyarrow array, without copying memory.
        """
    def __eq__(self, other: object) -> bool: ...
    @property
    def __geo_interface__(self) -> dict:
        """Implements the "geo interface protocol".

        See <https://gist.github.com/sgillies/2217756>
        """
    def __getitem__(self, key: int) -> Geometry:
        """Access the item at a given index"""
    def __len__(self) -> int:
        """The number of rows."""
    def __repr__(self) -> str:
        """Text representation"""
    @classmethod
    def from_arrow(cls, data: ArrowArrayExportable) -> Self:
        """Construct this object from existing Arrow data

        Args:
            input: Arrow array to use for constructing this object

        Returns:
            Self
        """
    @classmethod
    def from_arrow_pycapsule(
        cls, schema_capsule: object, array_capsule: object
    ) -> Self:
        """Construct this object from raw Arrow capsules."""
    @property
    def type(self) -> NativeType:
        """Get the geometry type of this array."""

class SerializedArray:
    """An immutable array of serialized geometries (WKB or WKT)."""
    def __init__(self, data: ArrowArrayExportable) -> None: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this
        array into a pyarrow array, without copying memory.
        """
    def __len__(self) -> int:
        """The number of rows."""
    def __repr__(self) -> str:
        """Text representation"""
    @classmethod
    def from_arrow(cls, data: ArrowArrayExportable) -> Self:
        """Construct this object from existing Arrow data

        Args:
            input: Arrow array to use for constructing this object

        Returns:
            Self
        """
    @classmethod
    def from_arrow_pycapsule(
        cls, schema_capsule: object, array_capsule: object
    ) -> Self:
        """Construct this object from raw Arrow capsules."""
    @property
    def type(self) -> SerializedType:
        """Get the type of this array."""

class ChunkedNativeArray:
    """
    An immutable chunked array of geometries using GeoArrow's in-memory representation.
    """
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.chunked_array()`][pyarrow.chunked_array] to
        convert this array into a pyarrow array, without copying memory.
        """
    def __eq__(self, other: object) -> bool: ...
    def __getitem__(self, key: int) -> Geometry:
        """Access the item at a given index."""
    def __len__(self) -> int:
        """The number of rows."""
    def __repr__(self) -> str:
        """Text representation."""
    def chunk(self, i: int) -> NativeArray:
        """Access a single underlying chunk."""
    def chunks(self) -> List[NativeArray]:
        """Convert to a list of single-chunked arrays."""
    def num_chunks(self) -> int:
        """Number of underlying chunks."""
    @classmethod
    def from_arrow(cls, data: ArrowArrayExportable) -> Self:
        """Construct this object from existing Arrow data

        Args:
            input: Arrow array to use for constructing this object

        Returns:
            Self
        """
    @classmethod
    def from_arrow_pycapsule(
        cls, schema_capsule: object, array_capsule: object
    ) -> Self:
        """Construct this object from raw Arrow capsules."""
    @property
    def type(self) -> NativeType:
        """Get the geometry type of this array."""

class NativeType:
    @overload
    def __init__(
        self,
        type: Literal[
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometry",
            "geometrycollection",
        ],
        dimension: Dimension | DimensionT,
        coord_type: CoordType | CoordTypeT,
    ) -> None: ...
    @overload
    def __init__(
        self,
        type: Literal["box"],
        dimension: Dimension | DimensionT,
        coord_type: None = None,
    ) -> None: ...
    def __init__(
        self,
        type: Literal[
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometry",
            "geometrycollection",
            "box",
        ],
        dimension: Dimension | DimensionT | None = None,
        coord_type: CoordType | CoordTypeT | None = None,
    ) -> None:
        """Create a new NativeType

        Args:
            type: The string type of the geometry. One of `"point"`, `"linestring"`,
                `"polygon"`, `"multipoint"`, `"multilinestring"`, `"multipolygon"`,
                `"geometry"`, `"geometrycollection"`, `"box"`.
            dimension: The coordinate dimension. Either "XY" or "XYZ". Defaults to None.
            coord_type: The coordinate type. Defaults to None.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.field()`][pyarrow.field] to
        convert this type into a pyarrow Field.
        """
    def __eq__(self, value: object) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowSchemaExportable) -> Self:
        """Construct this object from existing Arrow data

        Args:
            input: Arrow field to use for constructing this object

        Returns:
            Self
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> Self:
        """Construct this object from a raw Arrow schema capsule."""
    @property
    def coord_type(self) -> CoordType:
        """Get the coordinate type of this geometry type"""
    @property
    def dimension(self) -> Dimension:
        """Get the dimension of this geometry type"""

class SerializedType:
    def __init__(
        self,
        type: Literal["wkb", "wkt"],
    ) -> None:
        """Create a new SerializedType

        Args:
            type: The string type of the geometry. One of `"wkb"`.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.field()`][pyarrow.field] to
        convert this type into a pyarrow Field.
        """
    def __eq__(self, value: object) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, data: ArrowSchemaExportable) -> Self:
        """Construct this object from existing Arrow data

        Args:
            input: Arrow field to use for constructing this object

        Returns:
            Self
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> Self:
        """Construct this object from a raw Arrow schema capsule."""

@overload
def geometry_col(input: ArrowArrayExportable) -> NativeArray: ...
@overload
def geometry_col(input: ArrowStreamExportable) -> ChunkedNativeArray: ...
def geometry_col(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> NativeArray | ChunkedNativeArray:
    """Access the geometry column of a Table or RecordBatch

    Args:
        input: The Arrow RecordBatch or Table to extract the geometry column from.

    Returns:
        A geometry array or chunked array.
    """

# Interop

def read_pyogrio(
    path_or_buffer: Path | str | bytes,
    /,
    layer: int | str | None = None,
    encoding: str | None = None,
    columns: Sequence[str] | None = None,
    read_geometry: bool = True,
    skip_features: int = 0,
    max_features: int | None = None,
    where: str | None = None,
    bbox: Tuple[float, float, float, float] | Sequence[float] | None = None,
    mask=None,
    fids=None,
    sql: str | None = None,
    sql_dialect: str | None = None,
    return_fids=False,
    batch_size=65536,
    **kwargs,
) -> Table:
    """
    Read from an OGR data source to an Arrow Table

    Args:
        path_or_buffer: A dataset path or URI, or raw buffer.
        layer: If an integer is provided, it corresponds to the index of the layer
            with the data source. If a string is provided, it must match the name
            of the layer in the data source. Defaults to first layer in data source.
        encoding: If present, will be used as the encoding for reading string values from
            the data source, unless encoding can be inferred directly from the data
            source.
        columns: List of column names to import from the data source. Column names must
            exactly match the names in the data source, and will be returned in
            the order they occur in the data source. To avoid reading any columns,
            pass an empty list-like.
        read_geometry: If True, will read geometry into a GeoSeries. If False, a Pandas DataFrame
            will be returned instead. Default: `True`.
        skip_features: Number of features to skip from the beginning of the file before
            returning features. If greater than available number of features, an
            empty DataFrame will be returned. Using this parameter may incur
            significant overhead if the driver does not support the capability to
            randomly seek to a specific feature, because it will need to iterate
            over all prior features.
        max_features: Number of features to read from the file. Default: `None`.
        where: Where clause to filter features in layer by attribute values. If the data source
            natively supports SQL, its specific SQL dialect should be used (eg. SQLite and
            GeoPackage: [`SQLITE`][SQLITE], PostgreSQL). If it doesn't, the [`OGRSQL
            WHERE`][OGRSQL_WHERE] syntax should be used. Note that it is not possible to overrule
            the SQL dialect, this is only possible when you use the `sql` parameter.

            Examples: `"ISO_A3 = 'CAN'"`, `"POP_EST > 10000000 AND POP_EST < 100000000"`

            [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
            [OGRSQL_WHERE]: https://gdal.org/user/ogr_sql_dialect.html#where

        bbox: If present, will be used to filter records whose geometry intersects this
            box. This must be in the same CRS as the dataset. If GEOS is present
            and used by GDAL, only geometries that intersect this bbox will be
            returned; if GEOS is not available or not used by GDAL, all geometries
            with bounding boxes that intersect this bbox will be returned.
            Cannot be combined with `mask` keyword.
        mask: Shapely geometry, optional (default: `None`)
            If present, will be used to filter records whose geometry intersects
            this geometry. This must be in the same CRS as the dataset. If GEOS is
            present and used by GDAL, only geometries that intersect this geometry
            will be returned; if GEOS is not available or not used by GDAL, all
            geometries with bounding boxes that intersect the bounding box of this
            geometry will be returned. Requires Shapely >= 2.0.
            Cannot be combined with `bbox` keyword.
        fids : array-like, optional (default: `None`)
            Array of integer feature id (FID) values to select. Cannot be combined
            with other keywords to select a subset (`skip_features`,
            `max_features`, `where`, `bbox`, `mask`, or `sql`). Note that
            the starting index is driver and file specific (e.g. typically 0 for
            Shapefile and 1 for GeoPackage, but can still depend on the specific
            file). The performance of reading a large number of features usings FIDs
            is also driver specific.
        sql: The SQL statement to execute. Look at the sql_dialect parameter for more
            information on the syntax to use for the query. When combined with other
            keywords like `columns`, `skip_features`, `max_features`,
            `where`, `bbox`, or `mask`, those are applied after the SQL query.
            Be aware that this can have an impact on performance, (e.g. filtering
            with the `bbox` or `mask` keywords may not use spatial indexes).
            Cannot be combined with the `layer` or `fids` keywords.
        sql_dialect : str, optional (default: `None`)
            The SQL dialect the SQL statement is written in. Possible values:

            - **None**: if the data source natively supports SQL, its specific SQL dialect
                will be used by default (eg. SQLite and Geopackage: [`SQLITE`][SQLITE], PostgreSQL).
                If the data source doesn't natively support SQL, the [`OGRSQL`][OGRSQL] dialect is
                the default.
            - [`'OGRSQL'`][OGRSQL]: can be used on any data source. Performance can suffer
                when used on data sources with native support for SQL.
            - [`'SQLITE'`][SQLITE]: can be used on any data source. All [spatialite][spatialite]
                functions can be used. Performance can suffer on data sources with
                native support for SQL, except for Geopackage and SQLite as this is
                their native SQL dialect.

            [OGRSQL]: https://gdal.org/user/ogr_sql_dialect.html#ogr-sql-dialect
            [SQLITE]: https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect
            [spatialite]: https://www.gaia-gis.it/gaia-sins/spatialite-sql-latest.html

        **kwargs
            Additional driver-specific dataset open options passed to OGR. Invalid
            options will trigger a warning.

    Returns:
        Table
    """

def from_geopandas(input: gpd.GeoDataFrame) -> Table:
    """
    Create a GeoArrow Table from a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

    ### Notes:

    - Currently this will always generate a non-chunked GeoArrow array. This is partly because
    [pyarrow.Table.from_pandas][pyarrow.Table.from_pandas] always creates a single batch.

    Args:
        input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

    Returns:
        A GeoArrow Table
    """

def from_shapely(input, *, crs: CRSInput | None = None) -> NativeArray:
    """
    Create a GeoArrow array from an array of Shapely geometries.

    ### Notes:

    - Currently this will always generate a non-chunked GeoArrow array.
    - Under the hood, this will first call
        [`shapely.to_ragged_array`][], falling back to [`shapely.to_wkb`][] if
        necessary.

        This is because `to_ragged_array` is the fastest approach but fails on
        mixed-type geometries. It supports combining Multi-* geometries with
        non-multi-geometries in the same array, so you can combine e.g. Point and
        MultiPoint geometries in the same array, but `to_ragged_array` doesn't work if
        you have Point and Polygon geometries in the same array.

    Args:

    input: Any array object accepted by Shapely, including numpy object arrays and
    [`geopandas.GeoSeries`][geopandas.GeoSeries].

    Returns:

        A GeoArrow array
    """

@overload
def from_wkb(
    input: ArrowArrayExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> NativeArray: ...
@overload
def from_wkb(
    input: ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> ChunkedNativeArray: ...
def from_wkb(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> NativeArray | ChunkedNativeArray:
    """
    Parse an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.

    This will handle both ISO and EWKB flavors of WKB. Any embedded SRID in
    EWKB-flavored WKB will be ignored.

    Args:
        input: An Arrow array of Binary type holding WKB-formatted geometries.

    Other args:
        coord_type: Specify the coordinate type of the generated GeoArrow data.

    Returns:
        A GeoArrow-native geometry array
    """

@overload
def from_wkt(
    input: ArrowArrayExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> NativeArray: ...
@overload
def from_wkt(
    input: ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> ChunkedNativeArray: ...
def from_wkt(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    coord_type: CoordType | CoordTypeT = CoordType.Interleaved,
) -> NativeArray | ChunkedNativeArray:
    """
    Parse an Arrow StringArray from WKT to its GeoArrow-native counterpart.

    Args:
        input: An Arrow array of string type holding WKT-formatted geometries.

    Other args:
        coord_type: Specify the coordinate type of the generated GeoArrow data.

    Returns:
        A GeoArrow-native geometry array
    """

@overload
def to_wkt(input: ArrowArrayExportable) -> Array: ...
@overload
def to_wkt(input: ArrowStreamExportable) -> ChunkedArray: ...
def to_wkt(input: ArrowArrayExportable | ArrowStreamExportable) -> Array | ChunkedArray:
    """
    Encode a geometry array to WKT.

    Args:
        input: An Arrow array of string type holding WKT-formatted geometries.

    Returns:
        A GeoArrow-native geometry array
    """

def to_geopandas(input: ArrowStreamExportable) -> gpd.GeoDataFrame:
    """
    Convert a GeoArrow Table to a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].

    ### Notes:

    - This is an alias to [GeoDataFrame.from_arrow][geopandas.GeoDataFrame.from_arrow].

    Args:
    input: A GeoArrow Table.

    Returns:
        the converted GeoDataFrame
    """

def to_shapely(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> NDArray[np.object_]:
    """
    Convert a GeoArrow array to a numpy array of Shapely objects

    Args:
        input: input geometry array

    Returns:
        numpy array with Shapely objects
    """

@overload
def to_wkb(input: ArrowArrayExportable) -> NativeArray: ...
@overload
def to_wkb(input: ArrowStreamExportable) -> ChunkedNativeArray: ...
def to_wkb(input: ArrowArrayExportable) -> NativeArray:
    """
    Encode a GeoArrow-native geometry array to a WKBArray, holding ISO-formatted WKB geometries.

    Args:
        input: A GeoArrow-native geometry array

    Returns:
        An array with WKB-formatted geometries
    """
