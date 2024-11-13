from __future__ import annotations

from pathlib import Path
from typing import (
    BinaryIO,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from arro3.core import Schema, Table
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)
from geoarrow.rust.core import NativeArray
from pyproj import CRS

from .enums import GeoParquetEncoding
from .types import BboxCovering, GeoParquetEncodingT

class ParquetFile:
    def __init__(self, path: str, store: ObjectStore) -> None:
        """
        Construct a new ParquetFile

        This will synchronously fetch metadata from the provided path

        Args:
            path: a string URL to read from.
            store: the file system interface to read from.

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
        self, row_group_idx: int, bbox_paths: BboxCovering | None = None
    ) -> List[float]:
        """Get the bounds of a single row group.

        Args:
            row_group_idx: The row group index.
            bbox_paths: For files written with spatial partitioning, you don't need to pass in these column names, as they'll be specified in the metadata Defaults to None.

        Returns:
            The bounds of a single row group.
        """
    def row_groups_bounds(self, bbox_paths: BboxCovering | None = None) -> NativeArray:
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
        bbox: Sequence[int | float] | None = None,
        bbox_paths: BboxCovering | None = None,
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
        bbox: Sequence[int | float] | None = None,
        bbox_paths: BboxCovering | None = None,
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
    def __init__(self, paths: Sequence[str], store: ObjectStore) -> None:
        """
        Construct a new ParquetDataset

        This will synchronously fetch metadata from all listed files.

        Args:
            paths: a list of string URLs to read from.
            store: the file system interface to read from.

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
        bbox: Sequence[int | float] | None = None,
        bbox_paths: BboxCovering | None = None,
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
        bbox: Sequence[int | float] | None = None,
        bbox_paths: BboxCovering | None = None,
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
    store: Optional[ObjectStore] = None,
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
    store = ObjectStore('s3://bucket', options=options)
    table = read_flatgeobuf("path/in/bucket.fgb", store=store)
    ```

    Args:
        file: the path to the file or a Python file object in binary read mode.

    Other args:
        store: an ObjectStore instance for this url. This is required only if the file is at a remote
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
    store: Optional[ObjectStore] = None,
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
    store = ObjectStore('s3://bucket', options=options)
    table = await read_flatgeobuf_async("path/in/bucket.fgb", store=store)
    ```

    Args:
        path: the url or relative path to a remote FlatGeobuf file. If an argument is passed for
            `store`, this should be a path fragment relative to the root passed to the `ObjectStore`
            constructor.

    Other args:
        store: an ObjectStore instance for this url. This is required for non-HTTP urls.
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
    store: Optional[ObjectStore] = None,
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
    store = ObjectStore('s3://bucket', options=options)
    table = read_parquet("path/in/bucket.parquet", store=store)
    ```

    Args:
        path: the path to the file
        store: the ObjectStore to read from. Defaults to None.
        batch_size: the number of rows to include in each internal batch of the table.

    Returns:
        Table from GeoParquet file.
    """

async def read_parquet_async(
    path: Union[str, Path, BinaryIO],
    *,
    store: Optional[ObjectStore] = None,
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
    store = ObjectStore('s3://bucket', options=options)
    table = await read_parquet_async("path/in/bucket.parquet", store=store)
    ```

    Args:
        path: the path to the file
        store: the ObjectStore to read from. Defaults to None.
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

def read_shapefile(
    shp_file: Union[str, Path, BinaryIO], dbf_file: Union[str, Path, BinaryIO]
) -> Table:
    """
    Read a Shapefile into an Arrow Table.

    The returned Arrow table will have geometry information in native GeoArrow encoding.

    !!! note
        Coordinate Reference System information is not currently read from the Shapefile.

    Args:
        shp_file: the path to the `.shp` file or the `.shp` file as a Python file object in binary read mode.
        dbf_file: the path to the `.dbf` file or the `.dbf` file as a Python file object in binary read mode.
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
