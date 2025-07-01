from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, List, Literal, Sequence, TypedDict

from arro3.core import Array, Schema, Table
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)
from geoarrow.rust.core.types import CoordTypeInput
from obstore.store import ObjectStore
from pyproj import CRS

from .enums import GeoParquetEncoding
from .types import GeoParquetEncodingT

class PathInput(TypedDict):
    path: str
    """The path to the file."""

    size: int
    """The size of the file in bytes.

    If this is provided, only bounded range requests will be made instead of suffix
    requests. This is useful for object stores that do not support suffix requests, in
    particular Azure.
    """

class GeoParquetFile:
    @classmethod
    def open(cls, path: str | PathInput, store: ObjectStore) -> GeoParquetFile:
        """
        Open a Parquet file from the given path.

        This will synchronously fetch metadata from the provided path.

        Args:
            path: a string URL to read from.
            store: the object store interface to read from.
        """

    @classmethod
    async def open_async(
        cls,
        path: str | PathInput,
        store: ObjectStore,
    ) -> GeoParquetFile:
        """
        Open a Parquet file from the given path asynchronously.

        This will fetch metadata from the provided path in an async manner.

        Args:
            path: a string URL to read from.
            store: the object store interface to read from.
        """

    @property
    def num_rows(self) -> int:
        """The number of rows in this file."""
    @property
    def num_row_groups(self) -> int:
        """The number of row groups in this file."""
    def schema_arrow(
        self,
        *,
        parse_to_native: bool = True,
        coord_type: CoordTypeInput | None = None,
    ) -> Schema:
        """Access the Arrow schema of the generated data.

        Args:
            parse_to_native: If True, the schema will be parsed to native Arrow types.
                Defaults to True.
            coord_type: The coordinate type to use. Defaults to separated coordinates.
        """
    def crs(self, column_name: str | None = None) -> CRS | None:
        """Access the CRS of this file.

        Args:
            column_name: The geometry column name. If there is more than one geometry column in the file, you must specify which you want to read. Defaults to None.
        """
    def row_group_bounds(
        self,
        row_group_idx: int,
        column_name: str | None = None,
    ) -> List[float]:
        """Get the bounds of a single row group.

        Args:
            row_group_idx: The row group index.
            column_name: The geometry column name. If there is more than one geometry column in the file, you must specify which you want to read. Defaults to None.

        Returns:
            The bounds of a single row group.
        """
    def row_groups_bounds(
        self,
        column_name: str | None = None,
    ) -> Array:
        """
        Get the bounds of all row groups.

        As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be
        specified in the metadata.

        Args:
            column_name: The geometry column name. If there is more than one geometry column in the file, you must specify which you want to read. Defaults to None.

        Returns:
            A geoarrow "box" array with bounds of all row groups.
        """
    def file_bbox(self) -> List[float] | None:
        """
        Access the bounding box of the given column for the entire file

        If no column name is passed, retrieves the bbox from the primary geometry column.

        An error will be returned if the column name does not exist in the dataset.
        None will be returned if the metadata does not contain bounding box information.
        """
    async def read_async(
        self,
        *,
        bbox: Sequence[int | float] | None = None,
        parse_to_native: bool = True,
        coord_type: CoordTypeInput | None = None,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Table:
        """Perform an async read with the given options

        Keyword Args:
            bbox: The 2D bounding box to use for spatially-filtered reads. Requires the source GeoParquet dataset to be version 1.1 with either a bounding box column or native geometry encoding. Defaults to None.
            parse_to_native: If True, the data will be parsed to native Arrow types.
                Defaults to True.
            coord_type: The coordinate type to use. Defaults to separated coordinates.
            batch_size: The number of rows in each internal batch of the table.
                Defaults to 1024.
            limit: The maximum number of rows to read. Defaults to None, which means all rows will be read.
            offset: The number of rows to skip before starting to read. Defaults to None, which means no rows will be skipped.
        """
    def read(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        bbox: Sequence[int | float] | None = None,
        parse_to_native: bool = True,
        coord_type: CoordTypeInput | None = None,
    ) -> Table:
        """Perform a synchronous read with the given options

        Keyword Args:
            bbox: The 2D bounding box to use for spatially-filtered reads. Requires the source GeoParquet dataset to be version 1.1 with either a bounding box column or native geometry encoding. Defaults to None.
            parse_to_native: If True, the data will be parsed to native Arrow types.
                Defaults to True.
            coord_type: The coordinate type to use. Defaults to separated coordinates.
            batch_size: The number of rows in each internal batch of the table.
                Defaults to 1024.
            limit: The maximum number of rows to read. Defaults to None, which means all rows will be read.
            offset: The number of rows to skip before starting to read. Defaults to None, which means no rows will be skipped.
        """

class GeoParquetDataset:
    """An interface to read from a collection GeoParquet files with the same schema."""
    @classmethod
    def open(
        cls, paths: Sequence[str] | Sequence[PathInput], store: ObjectStore
    ) -> GeoParquetDataset:
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
    def schema_arrow(
        self,
        *,
        parse_to_native: bool = True,
        coord_type: CoordTypeInput | None = None,
    ) -> Schema:
        """Access the Arrow schema of the generated data.

        Args:
            parse_to_native: If True, the schema will be parsed to native Arrow types.
                Defaults to True.
            coord_type: The coordinate type to use. Defaults to separated coordinates.
        """
    def crs(self, column_name: str | None = None) -> CRS | None:
        """Access the CRS of this file.

        Args:
            column_name: The geometry column name. If there is more than one geometry column in the file, you must specify which you want to read. Defaults to None.
        """
    def fragment(self, path: str) -> GeoParquetFile:
        """Get a single file from this dataset."""
    @property
    def fragments(self) -> List[GeoParquetFile]:
        """Get the list of files in this dataset."""
    async def read_async(
        self,
        *,
        bbox: Sequence[int | float] | None = None,
        parse_to_native: bool = True,
        coord_type: CoordTypeInput | None = None,
        batch_size: int | None = None,
    ) -> Table:
        """Perform an async read with the given options

        Keyword Args:
            bbox: The 2D bounding box to use for spatially-filtered reads. Requires the source GeoParquet dataset to be version 1.1 with either a bounding box column or native geometry encoding. Defaults to None.
            parse_to_native: If True, the data will be parsed to native Arrow types.
                Defaults to True.
            coord_type: The coordinate type to use. Defaults to separated coordinates.
            batch_size: The number of rows in each internal batch of the table.
                Defaults to 1024.
        """

    def read(
        self,
        *,
        bbox: Sequence[int | float] | None = None,
        parse_to_native: bool = True,
        coord_type: CoordTypeInput | None = None,
        batch_size: int | None = None,
    ) -> Table:
        """Perform a sync read with the given options

        Keyword Args:
            bbox: The 2D bounding box to use for spatially-filtered reads. Requires the source GeoParquet dataset to be version 1.1 with either a bounding box column or native geometry encoding. Defaults to None.
            parse_to_native: If True, the data will be parsed to native Arrow types.
                Defaults to True.
            coord_type: The coordinate type to use. Defaults to separated coordinates.
            batch_size: The number of rows in each internal batch of the table.
                Defaults to 1024.
        """

class GeoParquetWriter:
    """Writer interface for a single GeoParquet file.

    This allows you to write GeoParquet files that are larger than memory.
    """
    def __init__(
        self,
        file: str | Path | BinaryIO,
        schema: ArrowSchemaExportable,
        *,
        encoding: GeoParquetEncoding | GeoParquetEncodingT = GeoParquetEncoding.WKB,
        compression: Literal["uncompressed", "snappy", "lzo", "lz4", "lz4_raw"]
        | str = "zstd(1)",
        writer_version: Literal["parquet_1_0", "parquet_2_0"] = "parquet_2_0",
    ) -> None:
        """
        Create a new GeoParquetWriter.

        !!! note
            This currently only supports writing to local files. Directly writing to object stores will be supported in a release soon.

        Args:
            file: the path to the file or a Python file object in binary write mode.
            schema: the Arrow schema of the data to write.

        Keyword Args:
            encoding: the geometry encoding to use. See [GeoParquetEncoding][geoarrow.rust.io.enums.GeoParquetEncoding] for more details on supported geometry encodings.
            compression: the compression algorithm to use. This can be either one of the strings in the `Literal` type, or a string that contains the compression level, like `gzip(9)` or `brotli(11)` or `zstd(22)`. The default is `zstd(1)`.
            writer_version: the Parquet writer version to use. Defaults to `"parquet_2_0"`.
        """
    def __enter__(self) -> GeoParquetWriter: ...
    def __exit__(self, type, value, traceback): ...
    def close(self) -> None:
        """Close this file.

        This is required to ensure that all data is flushed to disk and the file is properly finalized.

        The recommended use of this class is as a context manager, which will close the
        file automatically.
        """
    def is_closed(self) -> bool:
        """Returns `True` if the file has already been closed."""
    def write_batch(self, batch: ArrowArrayExportable) -> None:
        """Write a single RecordBatch to the GeoParquet file."""
    def write_table(self, table: ArrowArrayExportable | ArrowStreamExportable) -> None:
        """
        Write a table or stream of batches to the Parquet file

        This accepts an Arrow RecordBatch, Table, or RecordBatchReader. If a
        RecordBatchReader is passed, only one batch at a time will be materialized in
        memory, allowing you to write large datasets without running out of memory.

        Args:
            table: _description_
        """

# TODO: decide whether to keep these functions or not.

# def read_parquet(
#     path: Union[str, Path, BinaryIO],
#     *,
#     store: Optional[ObjectStore] = None,
#     batch_size: int = 65536,
# ) -> Table:
#     """
#     Read a GeoParquet file from a path on disk into an Arrow Table.

#     Example:

#     Reading from a local path:

#     ```py
#     from geoarrow.rust.io import read_parquet
#     table = read_parquet("path/to/file.parquet")
#     ```

#     Reading from an HTTP(S) url:

#     ```py
#     from geoarrow.rust.io import read_parquet

#     url = "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples/example.parquet"
#     table = read_parquet(url)
#     ```

#     Reading from a remote file on an S3 bucket.

#     ```py
#     from geoarrow.rust.io import ObjectStore, read_parquet

#     options = {
#         "aws_access_key_id": "...",
#         "aws_secret_access_key": "...",
#         "aws_region": "..."
#     }
#     store = ObjectStore('s3://bucket', options=options)
#     table = read_parquet("path/in/bucket.parquet", store=store)
#     ```

#     Args:
#         path: the path to the file
#         store: the ObjectStore to read from. Defaults to None.
#         batch_size: the number of rows to include in each internal batch of the table.

#     Returns:
#         Table from GeoParquet file.
#     """

# async def read_parquet_async(
#     path: Union[str, Path, BinaryIO],
#     *,
#     store: Optional[ObjectStore] = None,
#     batch_size: int = 65536,
# ) -> Table:
#     """
#     Read a GeoParquet file from a path on disk into an Arrow Table.

#     Examples:

#     Reading from an HTTP(S) url:

#     ```py
#     from geoarrow.rust.io import read_parquet_async

#     url = "https://raw.githubusercontent.com/opengeospatial/geoparquet/v1.0.0/examples/example.parquet"
#     table = await read_parquet_async(url)
#     ```

#     Reading from a remote file on an S3 bucket.

#     ```py
#     from geoarrow.rust.io import ObjectStore, read_parquet_async

#     options = {
#         "aws_access_key_id": "...",
#         "aws_secret_access_key": "...",
#         "aws_region": "..."
#     }
#     store = ObjectStore('s3://bucket', options=options)
#     table = await read_parquet_async("path/in/bucket.parquet", store=store)
#     ```

#     Args:
#         path: the path to the file

#     Keyword Args:
#         store: the ObjectStore to read from. Defaults to None.
#         batch_size: the number of rows to include in each internal batch of the table.

#     Returns:
#         Table from GeoParquet file.
#     """

# def write_parquet(
#     table: ArrowStreamExportable,
#     file: Union[str, Path, BinaryIO],
#     *,
#     encoding: GeoParquetEncoding | GeoParquetEncodingT = GeoParquetEncoding.WKB,
# ) -> None:
#     """
#     Write an Arrow RecordBatch, Table, or RecordBatchReader to a GeoParquet file on disk.

#     If a RecordBatchReader is passed, only one batch at a time will be materialized in memory.

#     !!! note
#         This currently only supports writing to local files. Directly writing to object stores will be supported in a release soon.

#     Args:
#         table: the table to write.
#         file: the path to the file or a Python file object in binary write mode.

#     Keyword Args:
#         encoding: the geometry encoding to use. See [GeoParquetEncoding][geoarrow.rust.io.enums.GeoParquetEncoding] for more details on supported geometry encodings.
#     """
