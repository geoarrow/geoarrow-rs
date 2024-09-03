from __future__ import annotations

from pathlib import Path
from typing import (
    BinaryIO,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from arro3.core import RecordBatchReader, Schema, Table
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)
from pyproj import CRS

from .enums import GeoParquetEncoding
from .types import (
    BboxPaths,
    GeoParquetEncodingT,
    IntFloat,
)

class ParquetFile:
    def __init__(self, path: str, fs: ObjectStore) -> None:
        """
        Construct a new ParquetFile

        This will synchronously fetch metadata from the provided path

        Args:
            path: a string URL to read from.
            fs: the file system interface to read from.

        Returns:
            A new ParquetFile object.
        """
    @property
    def num_rows(self) -> int:
        """The number of rows in this file."""
    @property
    def num_row_groups(self) -> int:
        """The number of row groups in this file."""
    @property
    def schema_arrow(self) -> Schema:
        """Access the Arrow schema of the generated data"""
    def crs(self, column_name: str | None = None) -> CRS:
        """Access the CRS of this file.

        Args:
            column_name: The geometry column name. If there is more than one geometry column in the file, you must specify which you want to read. Defaults to None.

        Returns:
            CRS
        """
    def row_group_bounds(
        self, row_group_idx: int, bbox_paths: BboxPaths | None = None
    ) -> List[float]:
        """Get the bounds of a single row group.

        Args:
            row_group_idx: The row group index.
            bbox_paths: For files written with spatial partitioning, you don't need to pass in these column names, as they'll be specified in the metadata Defaults to None.

        Returns:
            The bounds of a single row group.
        """
    def row_groups_bounds(self, bbox_paths: BboxPaths | None = None) -> GeometryArray:
        """
        Get the bounds of all row groups.

        As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be
        specified in the metadata.

        Args:
            bbox_paths: For files written with spatial partitioning, you don't need to pass in these column names, as they'll be specified in the metadata Defaults to None.

        Returns:
            A geoarrow "box" array with bounds of all row groups.
        """
    def file_bbox(self) -> List[float] | None:
        """
        Access the bounding box of the given column for the entire file

        If no column name is passed, retrieves the bbox from the primary geometry column.

        An Err will be returned if the column name does not exist in the dataset
        None will be returned if the metadata does not contain bounding box information.
        """
    async def read_async(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table:
        """Perform an async read with the given options

        Args:
            batch_size: _description_. Defaults to None.
            limit: _description_. Defaults to None.
            offset: _description_. Defaults to None.
            bbox: _description_. Defaults to None.
            bbox_paths: _description_. Defaults to None.

        Returns:
            _description_
        """
    def read(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table:
        """Perform a sync read with the given options

        Args:
            batch_size: _description_. Defaults to None.
            limit: _description_. Defaults to None.
            offset: _description_. Defaults to None.
            bbox: _description_. Defaults to None.
            bbox_paths: _description_. Defaults to None.

        Returns:
            _description_
        """

class ParquetDataset:
    def __init__(self, paths: Sequence[str], fs: ObjectStore) -> None:
        """
        Construct a new ParquetDataset

        This will synchronously fetch metadata from all listed files.

        Args:
            paths: a list of string URLs to read from.
            fs: the file system interface to read from.

        Returns:
            A new ParquetDataset object.
        """
    @property
    def num_rows(self) -> int:
        """The total number of rows across all files."""
    @property
    def num_row_groups(self) -> int:
        """The total number of row groups across all files"""
    @property
    def schema_arrow(self) -> Schema:
        """Access the Arrow schema of the generated data"""
    def crs(self, column_name: str | None = None) -> CRS:
        """Access the CRS of this file.

        Args:
            column_name: The geometry column name. If there is more than one geometry column in the file, you must specify which you want to read. Defaults to None.

        Returns:
            CRS
        """
    async def read_async(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table:
        """Perform an async read with the given options

        Args:
            batch_size: _description_. Defaults to None.
            limit: _description_. Defaults to None.
            offset: _description_. Defaults to None.
            bbox: _description_. Defaults to None.
            bbox_paths: _description_. Defaults to None.

        Returns:
            _description_
        """

    def read(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[IntFloat] | None = None,
        bbox_paths: BboxPaths | None = None,
    ) -> Table:
        """Perform a sync read with the given options

        Args:
            batch_size: _description_. Defaults to None.
            limit: _description_. Defaults to None.
            offset: _description_. Defaults to None.
            bbox: _description_. Defaults to None.
            bbox_paths: _description_. Defaults to None.

        Returns:
            _description_
        """

class ParquetWriter:
    """Writer interface for a single Parquet file.

    This allows you to write Parquet files that are larger than memory.
    """
    def __init__(
        self, file: str | Path | BinaryIO, schema: ArrowSchemaExportable
    ) -> None: ...
    def __enter__(self): ...
    def __exit__(self, type, value, traceback): ...
    def close(self) -> None:
        """
        Close this file.

        The recommended use of this class is as a context manager, which will close the
        file automatically.
        """
    def is_closed(self) -> bool:
        """Returns `True` if the file has already been closed."""
    def write_batch(self, batch: ArrowArrayExportable) -> None:
        """Write a single RecordBatch to the Parquet file"""
    def write_table(self, table: ArrowArrayExportable | ArrowStreamExportable) -> None:
        """
        Write a table or stream of batches to the Parquet file

        This accepts an Arrow RecordBatch, Table, or RecordBatchReader. If a
        RecordBatchReader is passed, only one batch at a time will be materialized in
        memory.

        Args:
            table: _description_
        """

class ObjectStore:
    def __init__(self, root: str, options: Optional[Dict[str, str]] = None) -> None: ...

def read_csv(
    file: str | Path | BinaryIO,
    geometry_column_name: str,
    *,
    batch_size: int = 65536,
) -> Table:
    """
    Read a CSV file from a path on disk into a Table.

    Args:
        file: the path to the file or a Python file object in binary read mode.
        geometry_column_name: the name of the geometry column within the CSV.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from CSV file.
    """

def read_flatgeobuf(
    file: Union[str, Path, BinaryIO],
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[float, float, float, float] | None = None,
) -> Table:
    """
    Read a FlatGeobuf file from a path on disk or a remote location into an Arrow Table.

    Example:

    Reading from a local path:

    ```py
    from geoarrow.rust.core import read_flatgeobuf
    table = read_flatgeobuf("path/to/file.fgb")
    ```

    Reading from a Python file object:

    ```py
    from geoarrow.rust.core import read_flatgeobuf

    with open("path/to/file.fgb", "rb") as file:
        table = read_flatgeobuf(file)
    ```

    Reading from an HTTP(S) url:

    ```py
    from geoarrow.rust.core import read_flatgeobuf

    url = "http://flatgeobuf.org/test/data/UScounties.fgb"
    table = read_flatgeobuf(url)
    ```

    Reading from a remote file on an S3 bucket.

    ```py
    from geoarrow.rust.core import ObjectStore, read_flatgeobuf

    options = {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "..."
    }
    fs = ObjectStore('s3://bucket', options=options)
    table = read_flatgeobuf("path/in/bucket.fgb", fs=fs)
    ```

    Args:
        file: the path to the file or a Python file object in binary read mode.

    Other args:
        fs: an ObjectStore instance for this url. This is required only if the file is at a remote
            location.
        batch_size: the number of rows to include in each internal batch of the table.
        bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy). If set to
        `None`, no spatial filtering will be performed.

    Returns:
        Table from FlatGeobuf file.
    """

async def read_flatgeobuf_async(
    path: str,
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
    bbox: Tuple[float, float, float, float] | None = None,
) -> Table:
    """
    Read a FlatGeobuf file from a url into an Arrow Table.

    Example:

    Reading from an HTTP(S) url:

    ```py
    from geoarrow.rust.core import read_flatgeobuf_async

    url = "http://flatgeobuf.org/test/data/UScounties.fgb"
    table = await read_flatgeobuf_async(url)
    ```

    Reading from an S3 bucket:

    ```py
    from geoarrow.rust.core import ObjectStore, read_flatgeobuf_async

    options = {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "..."
    }
    fs = ObjectStore('s3://bucket', options=options)
    table = await read_flatgeobuf_async("path/in/bucket.fgb", fs=fs)
    ```

    Args:
        path: the url or relative path to a remote FlatGeobuf file. If an argument is passed for
            `fs`, this should be a path fragment relative to the root passed to the `ObjectStore`
            constructor.

    Other args:
        fs: an ObjectStore instance for this url. This is required for non-HTTP urls.
        batch_size: the number of rows to include in each internal batch of the table.
        bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy). If set to
        `None`, no spatial filtering will be performed.

    Returns:
        Table from FlatGeobuf file.
    """

def read_geojson(file: Union[str, Path, BinaryIO], *, batch_size: int = 65536) -> Table:
    """
    Read a GeoJSON file from a path on disk into an Arrow Table.

    Args:
        file: the path to the file or a Python file object in binary read mode.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoJSON file.
    """

def read_geojson_lines(
    file: Union[str, Path, BinaryIO], *, batch_size: int = 65536
) -> Table:
    """
    Read a newline-delimited GeoJSON file from a path on disk into an Arrow Table.

    This expects a GeoJSON Feature on each line of a text file, with a newline character separating
    each Feature.

    Args:
        file: the path to the file or a Python file object in binary read mode.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoJSON file.
    """

def read_parquet(
    path: Union[str, Path, BinaryIO],
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
) -> Table:
    """
    Read a GeoParquet file from a path on disk into an Arrow Table.

    Example:

    Reading from a local path:

    ```py
    from geoarrow.rust.core import read_parquet
    table = read_parquet("path/to/file.parquet")
    ```

    Reading from an HTTP(S) url:

    ```py
    from geoarrow.rust.core import read_parquet

    url = "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples/example.parquet"
    table = read_parquet(url)
    ```

    Reading from a remote file on an S3 bucket.

    ```py
    from geoarrow.rust.core import ObjectStore, read_parquet

    options = {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "..."
    }
    fs = ObjectStore('s3://bucket', options=options)
    table = read_parquet("path/in/bucket.parquet", fs=fs)
    ```

    Args:
        path: the path to the file
        fs: the ObjectStore to read from. Defaults to None.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoParquet file.
    """

async def read_parquet_async(
    path: Union[str, Path, BinaryIO],
    *,
    fs: Optional[ObjectStore] = None,
    batch_size: int = 65536,
) -> Table:
    """
    Read a GeoParquet file from a path on disk into an Arrow Table.

    Examples:

    Reading from an HTTP(S) url:

    ```py
    from geoarrow.rust.core import read_parquet_async

    url = "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples/example.parquet"
    table = await read_parquet_async(url)
    ```

    Reading from a remote file on an S3 bucket.

    ```py
    from geoarrow.rust.core import ObjectStore, read_parquet_async

    options = {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
        "aws_region": "..."
    }
    fs = ObjectStore('s3://bucket', options=options)
    table = await read_parquet_async("path/in/bucket.parquet", fs=fs)
    ```

    Args:
        path: the path to the file
        fs: the ObjectStore to read from. Defaults to None.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoParquet file.
    """

def read_postgis(connection_url: str, sql: str) -> Optional[Table]:
    """
    Read a PostGIS query into an Arrow Table.

    Args:
        connection_url: _description_
        sql: _description_

    Returns:
        Table from query.
    """

async def read_postgis_async(connection_url: str, sql: str) -> Optional[Table]:
    """
    Read a PostGIS query into an Arrow Table.

    Args:
        connection_url: _description_
        sql: _description_

    Returns:
        Table from query.
    """

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
) -> RecordBatchReader:
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

def write_csv(table: ArrowStreamExportable, file: str | Path | BinaryIO) -> None:
    """
    Write a Table to a CSV file on disk.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """

def write_flatgeobuf(
    table: ArrowStreamExportable,
    file: str | Path | BinaryIO,
    *,
    write_index: bool = True,
) -> None:
    """
    Write to a FlatGeobuf file on disk.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Other args:
        write_index: whether to write a spatial index in the FlatGeobuf file. Defaults to True.
    """

def write_geojson(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None:
    """
    Write to a GeoJSON file on disk.

    Note that the GeoJSON specification mandates coordinates to be in the WGS84 (EPSG:4326)
    coordinate system, but this function will not automatically reproject into WGS84 for you.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """

def write_geojson_lines(
    table: ArrowStreamExportable, file: Union[str, Path, BinaryIO]
) -> None:
    """
    Write to a newline-delimited GeoJSON file on disk.

    Note that the GeoJSON specification mandates coordinates to be in the WGS84 (EPSG:4326)
    coordinate system, but this function will not automatically reproject into WGS84 for you.

    Args:
        table: the Arrow RecordBatch, Table, or RecordBatchReader to write.
        file: the path to the file or a Python file object in binary write mode.

    Returns:
        None
    """

def write_parquet(
    table: ArrowStreamExportable,
    file: Union[str, Path, BinaryIO],
    *,
    encoding: GeoParquetEncoding | GeoParquetEncodingT = GeoParquetEncoding.WKB,
) -> None:
    """
    Write an Arrow RecordBatch, Table, or RecordBatchReader to a GeoParquet file on disk.

    If a RecordBatchReader is passed, only one batch at a time will be materialized in memory.

    Args:
        table: the table to write.
        file: the path to the file or a Python file object in binary write mode.
        encoding: the geometry encoding to use. Defaults to `GeoParquetEncoding.WKB`.
    """
